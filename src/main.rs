mod api;
mod api_sql;
mod gafe;
mod s256;
mod sql_generate;
mod sql_test;
mod sync;
mod user_query;

use std::{
    collections::HashMap,
    future::{ready, IntoFuture},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::MatchedPath,
    routing::{get, post, Router},
};
use clap::Parser;
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{eyre, Result};
use futures::TryFutureExt;
use itertools::Itertools;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::str::FromStr;
use sync::{Config, Downloader};
use tower::ServiceBuilder;
use tower_http::{
    classify::ServerErrorsFailureClass, compression::CompressionLayer, cors::CorsLayer,
    trace::TraceLayer,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(long, env = "START_BLOCK")]
    start_block: Option<u64>,

    #[arg(long = "pg", env = "PG_URL", default_value = "postgres://localhost/ga")]
    pg_url: String,

    #[arg(long = "gafe-pg", env = "GAFE_PG_URL")]
    gafe_pg_url: Option<String>,

    #[arg(short = 'l', env = "LISTEN", default_value = "0.0.0.0:8000")]
    listen: String,

    #[arg(
        long = "pg-read-only-password",
        env = "PG_RO_PASSWORD",
        default_value = ""
    )]
    ro_password: String,

    #[clap(long, action = clap::ArgAction::SetTrue)]
    no_sync: bool,
}

static SCHEMA: &str = include_str!("./sql/schema.sql");

#[tokio::main]
async fn main() -> Result<(), api::Error> {
    let fmt_layer = fmt::layer()
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

    let args = Args::parse();
    let config = api::Config {
        pool: api_ro_pg(&args.pg_url, &args.ro_password),
        broadcaster: Arc::new(api::Broadcaster::default()),
        remote_broadcaster: Arc::new(api::Broadcaster2::default()),
        account_limits: Arc::new(Mutex::new(HashMap::new())),
        free_limit: Arc::new(gafe::AccountLimit::free()),
        open_limit: Arc::new(gafe::AccountLimit::open()),
        gafe: gafe::Connection::new(args.gafe_pg_url.clone()).await,
    };

    let prom_record = PrometheusBuilder::new()
        .add_global_label("name", "ga")
        .build_recorder();
    let prom_handler = prom_record.handle();
    metrics::set_global_recorder(TracingContextLayer::all().layer(prom_record))
        .expect("unable to set global metrics recorder");

    let tracing = TraceLayer::new_for_http()
        .make_span_with(|req: &axum::http::Request<Body>| {
             let path = req
                 .extensions()
                 .get::<MatchedPath>()
                 .map(MatchedPath::as_str);
             tracing::info_span!("http",
                 "api-key" = tracing::field::Empty,
                 "ip" = tracing::field::Empty,
                 "size" = tracing::field::Empty,
                 status = tracing::field::Empty,
                 path,
                 chain = tracing::field::Empty,
             )
        })
        .on_response(
            |resp: &axum::http::Response<_>, d: Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_u16());
                let _guard = span.enter();
                metrics::counter!("api.requests").increment(1);
                metrics::histogram!("api.latency").record(d.as_millis() as f64);
                if !resp.status().is_success() {
                    metrics::counter!("api.errors").increment(1);
                }
            },
        )
        .on_failure(
            |error: ServerErrorsFailureClass, _latency: Duration, _span: &tracing::Span| {
                tracing::error!(error = %error)
            },
        );

    let service = ServiceBuilder::new()
        .layer(axum::middleware::from_fn(api::latency_header))
        .layer(tracing)
        .layer(axum::middleware::from_fn(api::content_length_header))
        .layer(HandleErrorLayer::new(api::handle_service_error))
        .load_shed()
        .concurrency_limit(1024)
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn_with_state(
            config.clone(),
            api::limit,
        ))
        .layer(CompressionLayer::new());

    let app = Router::new()
        .route("/", get(|| async { "hello\n" }))
        .route("/metrics", get(move || ready(prom_handler.render())))
        .route("/status", get(api::handle_status))
        .route("/query", get(api_sql::handle_get))
        .route("/query", post(api_sql::handle_post))
        .route("/query-live", get(api_sql::handle_sse))
        .layer(service)
        .with_state(config.clone())
        .into_make_service_with_connect_info::<SocketAddr>();
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .expect("binding to tcp for http server");

    let res = tokio::try_join!(
        flatten(tokio::spawn(account_limits(config.clone()))),
        flatten(tokio::spawn(sync(
            args.clone(),
            config.remote_broadcaster.clone(),
            config.broadcaster.clone(),
        ))),
        flatten(tokio::spawn(
            axum::serve(listener, app)
                .into_future()
                .map_err(|e| eyre!("serving http: {}", e))
        )),
    );
    match res {
        Err(err) => panic!("{}", err),
        _ => Ok(()),
    }
}

async fn sync(
    args: Args,
    remote_broadcaster: Arc<api::Broadcaster2>,
    broadcaster: Arc<api::Broadcaster>,
) -> Result<()> {
    if args.no_sync {
        tracing::info!("no sync");
        return Ok(());
    }
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let pg_mgr = Manager::from_config(
        args.pg_url.parse().expect("parsing pg arg"),
        connector,
        ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );
    let pg_pool = Pool::builder(pg_mgr)
        .max_size(16)
        .build()
        .expect("unable to build new ro pool");
    let pg = pg_pool.get().await?;
    pg.batch_execute(SCHEMA).await.unwrap();
    let tasks = Config::load(&pg)
        .await?
        .into_iter()
        .filter(|c| c.enabled)
        .map(|config| {
            let ch = broadcaster.clone();
            let rb = remote_broadcaster.clone();
            let pg = pg_pool.clone();
            tokio::spawn(async move {
                Downloader::new(pg, rb, config, args.start_block)
                    .run(ch)
                    .await
            })
        })
        .collect_vec();
    for t in tasks {
        if let Err(e) = t.await {
            return Err(e.into());
        }
    }
    Ok(())
}

fn api_ro_pg(cstr: &str, ro_password: &str) -> Pool {
    let mut pg_config = tokio_postgres::Config::from_str(cstr).expect("unable to connect to ro pg");
    pg_config.user("uapi");
    pg_config.password(ro_password);
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

async fn account_limits(config: api::Config) -> Result<()> {
    loop {
        if let Some(limits) = config.gafe.load_account_limits().await {
            *config.account_limits.lock().unwrap() = limits;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(eyre!("handle error: {}", err)),
    }
}
