use axum::{
    body::Body,
    extract::{connect_info::IntoMakeServiceWithConnectInfo, MatchedPath},
    http::{StatusCode, Uri},
    routing::{get, post},
    Router,
};
use axum_extra::extract::cookie::Key;
use clap::{command, Parser};
use eyre::{Context, Result};
use fe::{
    account, api_docs, api_key, conduit_api, god_mode, postmark, query, session, stripe, web,
};
use rust_embed::Embed;
use std::{collections::HashMap, net::SocketAddr};
use tower_http::trace::TraceLayer;
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
    };
    state.pool.get().await?.batch_execute(SCHEMA).await?;

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

    handlebars::handlebars_helper!(trunc: |s: String, n: usize| s.chars().take(n).collect::<String>());
    handlebars::handlebars_helper!(join: |s: Vec<String>, sep: String| s.join(&sep));
    handlebars::handlebars_helper!(snippet: |s: String, file: String| get_snippet(s, file));

    let mut reg = handlebars::Handlebars::new();
    reg.register_helper("trunc", Box::new(trunc));
    reg.register_helper("join", Box::new(join));
    reg.register_helper("snippet", Box::new(snippet));
    reg.set_dev_mode(true);
    reg.register_embed_templates::<Assets>()?;
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
            |resp: &axum::http::Response<_>, d: std::time::Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_str());
                let _guard = span.enter();
                metrics::counter!("api.requests").increment(1);
                metrics::histogram!("api.latency").record(d.as_millis() as f64);
                if resp.status().is_client_error() || resp.status().is_server_error() {
                    metrics::counter!("api.errors").increment(1);
                }
            },
        );
    let service = tower::ServiceBuilder::new().layer(tracing);
    Router::new()
        .route("/", get(account::handlers::index))
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
        .route("/change-plan", post(account::handlers::change_plan))
        .route("/new-api-key", get(api_key::handlers::new))
        .route("/create-api-key", post(api_key::handlers::create))
        .route("/delete-api-key", post(api_key::handlers::delete))
        .route("/conduit/add-chain", post(conduit_api::add))
        .fallback(fallback)
        .layer(service)
        .with_state(state)
        .into_make_service_with_connect_info::<SocketAddr>()
}

#[cfg(test)]
mod tests {
    use super::{service, templates, web, SCHEMA};
    use axum_flash::Key;
    use axum_test::TestServer;
    use deadpool_postgres::Pool;
    use fe::{conduit_api, postmark, stripe};

    fn test_state(pool: Pool) -> web::State {
        web::State {
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
        }
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
    async fn test_conduit_add() {
        let pool = shared::pg::test::new(SCHEMA).await;
        let request = conduit_api::CreateRequest {
            id: String::from("foo"),
            event: String::from("INSTALLED"),
            chain_id: 42,
            rpc: String::from("/foo"),
        };
        let server = TestServer::new(service(test_state(pool.clone()))).unwrap();
        server
            .post("/conduit/add-chain")
            .json(&request)
            .await
            .assert_json(&serde_json::json!({"id": "foo", "status": "INSTALLED"}));

        let resp = server.post("/conduit/add-chain").json(&request).await;
        resp.assert_status_not_ok();
        resp.assert_json(&serde_json::json!({"message": "duplicate for chain: 42"}));
        let resp = server
            .post("/conduit/add-chain")
            .json(&conduit_api::CreateRequest {
                id: String::from("foo"),
                event: String::from("INSTALLED"),
                chain_id: 43,
                rpc: String::from("/foo"),
            })
            .await;
        resp.assert_status_not_ok();
        resp.assert_json(&serde_json::json!({"message": "duplicate for id: foo"}));
    }
}
