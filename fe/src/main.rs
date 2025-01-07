use axum::{
    body::Body,
    extract::MatchedPath,
    http::{StatusCode, Uri},
    routing::{get, post},
    Router,
};
use axum_extra::extract::cookie::Key;
use clap::{command, Parser};
use eyre::{Context, Result};
use fe::{
    account, api_docs, api_key, conduit_api, god_mode, pg, postmark, query, session, stripe, web,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use rust_embed::Embed;
use std::{collections::HashMap, future::ready, net::SocketAddr};
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
        .with(MetricsLayer::new())
        .with(fmt_layer)
        .with(filter_layer)
        .init();
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
    let prom_record = PrometheusBuilder::new()
        .add_global_label("name", "dozer")
        .build_recorder();
    let prom_handler = prom_record.handle();
    metrics::set_global_recorder(TracingContextLayer::all().layer(prom_record))
        .expect("unable to set global metrics recorder");

    let args = Args::parse();
    let session_key = if let Some(key) = &args.session_key {
        Key::from(&hex::decode(key).wrap_err("unable to hex decode session key")?)
    } else {
        let k = Key::generate();
        tracing::info!("creating new session key: {}", hex::encode(k.master()));
        k
    };

    #[derive(Embed)]
    #[folder = "src/static"]
    #[include = "*.html"]
    #[include = "*.md"]
    struct Assets;

    handlebars::handlebars_helper!(trunc: |s: String, n: usize| s.chars().take(n).collect::<String>());
    handlebars::handlebars_helper!(join: |s: Vec<String>, sep: String| s.join(&sep));

    let mut reg = handlebars::Handlebars::new();
    reg.register_helper("trunc", Box::new(trunc));
    reg.register_helper("join", Box::new(join));
    reg.set_dev_mode(true);
    reg.register_embed_templates::<Assets>()?;

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
        templates: reg,
        pool: pg::new_pool(&args.pg_url, 16).expect("unable to create pg pool"),
        flash: axum_flash::Config::new(Key::generate()).use_secure_cookies(false),
        postmark: postmark::Client::new(args.postmark_key),
        stripe: stripe::Client::new(args.stripe_key),
        stripe_pub_key: args.stripe_pub_key,
    };
    state.pool.get().await?.batch_execute(SCHEMA).await?;

    let service = tower::ServiceBuilder::new().layer(tracing);
    let app = Router::new()
        .route("/", get(account::handlers::index))
        .route("/godmode", get(god_mode::index))
        .route("/docs", get(api_docs::index))
        .route("/query", get(account::handlers::index))
        .route("/metrics", get(move || ready(prom_handler.render())))
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
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8001").await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn fallback(uri: Uri) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, format!("No route for {uri}"))
}
