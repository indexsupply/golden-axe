mod email;
mod session;
mod web;

use axum::{
    body::Body,
    extract::MatchedPath,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use axum_extra::extract::{cookie::Key, SignedCookieJar};
use clap::{command, Parser};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::Result;
use maud::html;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::{future::ready, net::SocketAddr, str::FromStr};
use tower_http::trace::TraceLayer;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static SCHEMA: &str = include_str!("./schema.sql");

#[derive(Parser)]
#[command(name = "gafe", about = "A front end for Golden Axe", version = "0.1")]
struct Args {
    #[arg(long, env = "PG_URL", default_value = "postgres://localhost/gafe")]
    pg_url: String,

    #[arg(long, env = "SENDGRID_KEY")]
    sendgrid_key: String,

    #[arg(long, env = "HOST", default_value = "localhost:8001")]
    host: String,
}

fn pg_pool(pg_url: &str) -> Pool {
    let pg_config = tokio_postgres::Config::from_str(pg_url).expect("unable to connect to pg");
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let pg_mgr = Manager::from_config(
        pg_config,
        connector,
        ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );
    Pool::builder(pg_mgr)
        .max_size(16)
        .build()
        .expect("unable to build new ro pool")
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
    let state = web::State {
        key: Key::generate(),
        pool: pg_pool(&args.pg_url),
        flash: axum_flash::Config::new(Key::generate()).use_secure_cookies(false),
        sendgrid: email::Client {
            host: args.host,
            key: args.sendgrid_key,
        },
    };
    state.pool.get().await?.batch_execute(SCHEMA).await?;

    let service = tower::ServiceBuilder::new().layer(tracing);
    let app = Router::new()
        .route("/", get(index))
        .route("/metrics", get(move || ready(prom_handler.render())))
        .route("/login", get(session::try_login))
        .route("/email-login-link", get(session::login))
        .route("/email-login-link", post(session::email_login_link))
        .route("/logout", get(session::logout))
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

async fn index(flash: axum_flash::IncomingFlashes, jar: SignedCookieJar) -> impl IntoResponse {
    let resp = html! {
        @for (_level, message) in &flash {
            p {(message)}
        }
        @match session::User::from_jar(jar) {
            Some(u) => span {"hi: " (u.email)},
            None => span { "please log in" }
        }

    };
    (flash, resp).into_response()
}
