use axum::{
    body::Body,
    extract::{connect_info::IntoMakeServiceWithConnectInfo, MatchedPath},
    http::{StatusCode, Uri},
    routing::{get, post},
    Router,
};
use axum_extra::extract::cookie::Key;
use base64::{prelude::BASE64_STANDARD, Engine};
use clap::{command, Parser};
use eyre::{Context, Result};
use fe::{
    account, api_docs, api_key, chains, daimo, god_mode, postmark, query, session, stripe, web,
    whitelabel,
};
use rust_embed::Embed;
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static SCHEMA: &str = include_str!("./schema.sql");

#[derive(Parser)]
#[command(name = "fe", about = "A front end for Golden Axe", version = "0.1")]
struct Args {
    #[arg(
        long = "pg-fe",
        env = "PG_URL_FE",
        default_value = "postgres://localhost/fe"
    )]
    pg_url: String,

    #[arg(long, env = "BE_URL", default_value = "https://api.indexsupply.net")]
    be_url: String,

    #[arg(
        long,
        help = "included in generated links",
        env = "FE_URL",
        default_value = "http://localhost:8001"
    )]
    fe_url: String,

    #[arg(long, env = "POSTMARK_KEY")]
    postmark_key: Option<String>,

    #[arg(long, env = "STRIPE_KEY")]
    stripe_key: Option<String>,

    #[arg(long, env = "STRIPE_PUB_KEY")]
    stripe_pub_key: Option<String>,

    #[arg(long, env = "SESSION_KEY")]
    session_key: Option<String>,

    #[arg(long, env = "DAIMO_KEY")]
    daimo_key: Option<String>,

    #[arg(long, env = "INDEXSUPPLY_KEY")]
    indexsupply_key: Option<String>,

    #[clap(
        env = "ADMIN_API_SECRET",
        default_value = "2d6f3071fcf70f5731575be2f407b4ef"
    )]
    admin_api_secret: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .compact();
    let filter_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(filter_layer)
        .init();

    let args = Args::parse();
    let session_key = if let Some(key) = &args.session_key {
        Key::from(&hex::decode(key).wrap_err("unable to hex decode session key")?)
    } else {
        let k = Key::generate();
        tracing::info!("creating new session key: {}", hex::encode(k.master()));
        k
    };

    let example: HashMap<String, Vec<query::Query>> =
        toml::from_str(include_str!("../examples.toml")).expect("unable to read toml");
    let examples = example
        .get("queries")
        .expect("missing queries from example toml")
        .clone();

    let state = web::State {
        admin_api_secret: args.admin_api_secret,
        examples,
        be_url: args.be_url,
        fe_url: args.fe_url,
        key: session_key,
        templates: templates()?,
        pool: shared::pg::new_pool(&args.pg_url, 16).expect("unable to create pg pool"),
        flash: axum_flash::Config::new(Key::generate()).use_secure_cookies(false),
        postmark: postmark::Client::new(args.postmark_key),
        stripe: stripe::Client::new(args.stripe_key),
        stripe_pub_key: args.stripe_pub_key,
        daimo: daimo::Client::new(args.daimo_key, args.indexsupply_key.clone()),
    };
    state.pool.get().await?.batch_execute(SCHEMA).await?;

    tokio::spawn(update_daily_user_queries(state.pool.clone()));
    tokio::spawn(update_wl_daily_user_queries(state.pool.clone()));
    tokio::spawn(cleanup_user_queries(state.pool.clone()));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8001").await?;
    axum::serve(listener, service(state)).await?;
    Ok(())
}

async fn fallback(uri: Uri) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, format!("No route for {uri}"))
}

fn templates() -> Result<handlebars::Handlebars<'static>, handlebars::TemplateError> {
    #[derive(Embed)]
    #[folder = "src/static"]
    #[include = "*.html"]
    #[include = "*.md"]
    struct Assets;

    fn get_snippet(snippet_name: String, file_name: String) -> String {
        let key = format!("!!!{}", snippet_name);
        let file = Assets::get(&file_name).unwrap().data;
        let contents = std::str::from_utf8(&file).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        let mut indices = Vec::new();

        for (i, line) in lines.iter().enumerate() {
            if line.contains(&key) {
                indices.push(i);
            }
        }

        if let (Some(&start), Some(&end)) = (indices.first(), indices.get(1)) {
            let mut result = lines[start + 1..end].to_vec();
            if let Some(index) = lines[end].find(&key) {
                result.push(&lines[end][..index]);
            }
            result.join("\n")
        } else {
            format!("snippet {} not found", snippet_name)
        }
    }

    handlebars::handlebars_helper!(commas: |a: i32| account::with_commas(a.into()));
    handlebars::handlebars_helper!(gt: |a: i32, b: i32| a.gt(&b));
    handlebars::handlebars_helper!(eq: |a: i32, b: i32| a.eq(&b));
    handlebars::handlebars_helper!(trunc: |s: String, n: usize| s.chars().take(n).collect::<String>());
    handlebars::handlebars_helper!(join: |s: Vec<String>, sep: String| s.join(&sep));
    handlebars::handlebars_helper!(snippet: |s: String, file: String| get_snippet(s, file));
    handlebars::handlebars_helper!(money: |i: i64| account::money(i));
    handlebars::handlebars_helper!(btoa: |s: String| BASE64_STANDARD.encode(s));
    handlebars::handlebars_helper!(atob: |s: String| BASE64_STANDARD.decode(s).unwrap_or_default());

    let mut reg = handlebars::Handlebars::new();
    if cfg!(debug_assertions) {
        reg.set_dev_mode(true);
    }
    reg.register_helper("commas", Box::new(commas));
    reg.register_helper("gt", Box::new(gt));
    reg.register_helper("trunc", Box::new(trunc));
    reg.register_helper("join", Box::new(join));
    reg.register_helper("snippet", Box::new(snippet));
    reg.register_helper("money", Box::new(money));
    reg.register_helper("btoa", Box::new(btoa));
    reg.register_helper("atob", Box::new(atob));
    reg.register_embed_templates::<Assets>()?;
    reg.register_escape_fn(handlebars::no_escape);
    Ok(reg)
}

fn service(state: web::State) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr> {
    let tracing = TraceLayer::new_for_http()
        .make_span_with(|req: &axum::http::Request<Body>| {
            let path = req
                .extensions()
                .get::<MatchedPath>()
                .map(MatchedPath::as_str);
            tracing::info_span!("http", path, status = tracing::field::Empty)
        })
        .on_response(
            |resp: &axum::http::Response<_>, _: Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_str());
            },
        )
        .on_failure(
            |error: ServerErrorsFailureClass, _: Duration, _span: &tracing::Span| {
                tracing::error!(error = %error)
            },
        );
    let service = tower::ServiceBuilder::new().layer(tracing);
    Router::new()
        .route("/", get(account::handlers::index))
        .route("/status", get(web::status))
        .route("/godmode", get(god_mode::index))
        .route("/docs", get(api_docs::index))
        .route("/query", get(account::handlers::index))
        .route("/login", get(session::try_login))
        .route(
            "/login-form.js",
            get(|| async { include_str!("static/login-form.js") }),
        )
        .route("/email-login-link", get(session::login))
        .route("/email-login-link", post(session::email_login_link))
        .route("/logout", get(session::logout))
        .route("/account", get(account::handlers::account))
        .route("/query-history", get(query::handlers::list))
        .route("/setup-daimo", post(account::handlers::setup_daimo))
        .route("/setup-stripe", post(account::handlers::setup_stripe))
        .route("/update-stripe", post(account::handlers::update_stripe))
        .route("/update-limit", post(account::handlers::update_limit))
        .route("/new-api-key", get(api_key::handlers::new))
        .route("/create-api-key", post(api_key::handlers::create))
        .route("/show-api-key", post(api_key::handlers::show))
        .route("/edit-api-key", post(api_key::handlers::edit))
        .route("/update-api-key", post(api_key::handlers::update))
        .route("/delete-api-key", post(api_key::handlers::delete))
        .route("/wl/add-chain", post(chains::handlers::add))
        .route("/wl/list-chains", get(chains::handlers::list))
        .route("/wl/enable-chain", post(chains::handlers::enable))
        .route("/wl/disable-chain", post(chains::handlers::disable))
        .route("/wl/create-api-key", post(whitelabel::create_key))
        .route("/wl/list-api-keys", post(whitelabel::list_keys))
        .route("/wl/delete-api-key", post(whitelabel::delete_key))
        .route(
            "/wl/update-api-key-origins",
            post(whitelabel::update_origins),
        )
        .route(
            "/wl/update-api-key-hard-limit",
            post(whitelabel::update_hard_limit),
        )
        .route("/wl/usage", post(whitelabel::usage))
        .fallback(fallback)
        .layer(service)
        .with_state(state)
        .into_make_service_with_connect_info::<SocketAddr>()
}

#[tracing::instrument(skip_all)]
async fn update_daily_user_queries(pool: deadpool_postgres::Pool) {
    loop {
        let pool = pool.clone();
        let result = tokio::spawn(async move {
            pool.get()
                .await
                .expect("getting pg from pool")
                .query(
                    "
                    insert into daily_user_queries (owner_email, day, n, updated_at)
                    select
                        k.owner_email,
                        date_trunc('day', q.created_at)::date as day,
                        sum(qty)::int8,
                        now()
                    from user_queries q
                    join api_keys k on q.api_key = k.secret
                    where q.created_at >= date_trunc('day', now())
                      and q.created_at < date_trunc('day', now() + interval '1 day')
                    group by k.owner_email, date_trunc('day', q.created_at)::date
                    on conflict (owner_email, day)
                    do update set n = excluded.n, updated_at = excluded.updated_at;
                    ",
                    &[],
                )
                .await
                .expect("updating records");
        })
        .await;
        match result {
            Ok(_) => tracing::info!("updated"),
            Err(e) => tracing::error!("{}", e),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[tracing::instrument(skip_all)]
async fn update_wl_daily_user_queries(pool: deadpool_postgres::Pool) {
    loop {
        let pool = pool.clone();
        let result = tokio::spawn(async move {
            pool.get()
                .await
                .expect("getting pg from pool")
                .query(
                    "
                    insert into wl_daily_user_queries (provision_key, org, day, n, updated_at)
                    select
                        k.provision_key,
                        k.org,
                        date_trunc('day', q.created_at)::date as day,
                        sum(qty)::int8,
                        now()
                    from user_queries q
                    join wl_api_keys k on q.api_key = k.secret
                    where q.created_at >= date_trunc('day', now())
                      and q.created_at < date_trunc('day', now() + interval '1 day')
                    group by k.provision_key, k.org, date_trunc('day', q.created_at)::date
                    on conflict (provision_key, org, day)
                    do update set n = excluded.n, updated_at = excluded.updated_at;
                    ",
                    &[],
                )
                .await
                .expect("updating records");
        })
        .await;
        match result {
            Ok(_) => tracing::info!("updated"),
            Err(e) => tracing::error!("{}", e),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[tracing::instrument(skip_all)]
async fn cleanup_user_queries(pool: deadpool_postgres::Pool) {
    loop {
        let pool = pool.clone();
        let task = tokio::spawn(async move {
            pool.get()
                .await
                .expect("getting pg from pool")
                .execute(
                    "delete from user_queries where created_at < now() - '45 days'::interval",
                    &[],
                )
                .await
                .expect("deleting records");
        })
        .await;
        match task {
            Ok(_) => tracing::info!("updated"),
            Err(e) => tracing::error!("{}", e),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{service, templates, web, SCHEMA};
    use axum_flash::Key;
    use axum_test::TestServer;
    use deadpool_postgres::Pool;
    use fe::{daimo, postmark, stripe};

    fn test_state(pool: Pool) -> web::State {
        web::State {
            admin_api_secret: String::new(),
            pool,
            examples: Vec::new(),
            be_url: String::new(),
            fe_url: String::new(),
            flash: axum_flash::Config::new(Key::generate()).use_secure_cookies(false),
            templates: templates().expect("handlebars templates"),
            key: Key::generate(),
            postmark: postmark::Client::default(),
            stripe: stripe::Client::default(),
            stripe_pub_key: None,
            daimo: daimo::Client::default(),
        }
    }

    async fn delete_chain(pg: &tokio_postgres::Client, chain: u64) {
        pg.execute("delete from config where chain = $1", &[&(chain as i64)])
            .await
            .expect("unable to delete chain");
    }

    #[tokio::test]
    async fn test_index() {
        let pool = shared::pg::test::new(SCHEMA).await;
        let server = TestServer::new(service(test_state(pool.clone()))).unwrap();
        server
            .get("/")
            .await
            .assert_text_contains("<title>Index Supply</title>");
    }

    #[tokio::test]
    async fn test_add_chain() {
        let pool = shared::pg::test::new(SCHEMA).await;
        let pg = pool.get().await.expect("getting pg from pool");
        delete_chain(&pg, 1).await;
        let request = serde_json::json!({
            "name": "Main",
            "chain": 1,
            "url": "https://eth.merkle.io"
        });
        let server = TestServer::new(service(test_state(pool.clone()))).unwrap();
        server
            .post("/wl/add-chain")
            .json(&request)
            .await
            .assert_status_ok();
        let resp = server.post("/wl/add-chain").json(&request).await;
        resp.assert_status_not_ok();
        resp.assert_json(&serde_json::json!({"message": "duplicate for chain: 1"}));
    }
}
