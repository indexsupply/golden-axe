mod api;
mod api_sql;
mod backup;
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

use alloy::{
    primitives::U64,
    providers::{Provider, ProviderBuilder},
    rpc::types::eth::{BlockNumberOrTag, Filter},
};
use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::MatchedPath,
    routing::{get, post, Router},
};
use clap::{Parser, Subcommand};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{eyre, Context, Result};
use futures::TryFutureExt;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::str::FromStr;
use sync::Downloader;
use tower::ServiceBuilder;
use tower_http::{
    classify::ServerErrorsFailureClass, compression::CompressionLayer, cors::CorsLayer,
    trace::TraceLayer,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use url::Url;

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(name = "backup", about = "Pg_dump then upload to s3")]
    Backup(ServerArgs),
    #[command(name = "restore", about = "Download from s3 and then pg_restore")]
    Restore(ServerArgs),

    #[command(name = "server", about = "Serve API requests and sync decoded logs")]
    Server(ServerArgs),

    #[command(name = "query", about = "Query decoded logs", long_about = Some(api_sql::cli::HELP))]
    Query(api_sql::cli::Request),
}

#[derive(Parser)]
#[command(name = "ga", about = "The final indexer", version = "0.1")]
struct GlobalArgs {
    #[command(subcommand)]
    command: Commands,
    #[clap(
        long = "url",
        global = true,
        env = "GA_URL",
        help = "url to golden axe http api",
        default_value = "http://golden-axe-1:8000"
    )]
    url: Url,
}

#[derive(Clone, Parser, Debug)]
struct ServerArgs {
    #[arg(long, env = "START_BLOCK")]
    start_block: Option<u64>,

    #[arg(long, env = "STOP_BLOCK")]
    stop_block: Option<u64>,

    #[arg(short, long, default_value = "1000", env = "BATCH_SIZE")]
    batch_size: u64,

    #[arg(short, long, default_value = "1", env = "CONCURRENCY")]
    concurrency: u64,

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

    #[arg(
        long = "eth",
        env = "ETH_URL",
        default_value = "https://base-rpc.publicnode.com"
    )]
    eth_url: Url,

    #[clap(long, action = clap::ArgAction::SetTrue)]
    no_sync: bool,

    #[clap(long, env = "NO_BACKUP", action = clap::ArgAction::SetTrue)]
    no_backup: bool,

    #[clap(
        long = "backup-bucket",
        env = "GA_BACKUP_BUCKET",
        default_value = "ga-pg-backup"
    )]
    backup_bucket: String,

    #[clap(long = "backup-dir", env = "GA_BACKUP_DIR", default_value = ".")]
    backup_dir: String,

    #[clap(long = "backup-window", default_value = "1 day")]
    backup_window: humantime::Duration,

    #[clap(long = "restore-key", help = "the s3 key to use as restore")]
    restore_key: Option<String>,

    #[clap(long = "restore-chain-id", help = "restoring a particular chain")]
    restore_chain_id: Option<u64>,
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

    let args = GlobalArgs::parse();
    match args.command {
        Commands::Backup(args) => {
            backup::backup(
                &args.pg_url,
                &args.backup_dir,
                &args.backup_bucket,
                args.backup_window,
            )
            .await?
        }
        Commands::Restore(args) => {
            backup::restore(
                &args.pg_url,
                args.restore_chain_id.expect("missing chain id"),
                &args.backup_dir,
                &args.backup_bucket,
                args.restore_key,
            )
            .await?
        }
        Commands::Query(args) => api_sql::cli::request(&reqwest::Client::new(), args).await?,
        Commands::Server(args) => server(args).await,
    }
    Ok(())
}

async fn sync(args: ServerArgs, broadcaster: Arc<api::Broadcaster>) -> Result<()> {
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
    pg_pool
        .get()
        .await
        .wrap_err("getting pg conn from pool")?
        .batch_execute(SCHEMA)
        .await
        .unwrap();

    let eth_client = ProviderBuilder::new().on_http(args.eth_url.clone());
    let start = match args.start_block {
        Some(n) => BlockNumberOrTag::Number(n),
        None => BlockNumberOrTag::Latest,
    };
    let chain_id = eth_client.get_chain_id().await?;
    pg_pool
        .get()
        .await
        .wrap_err("getting pg conn from pool")?
        .execute(
            "insert into config(chain_id) values($1) on conflict (chain_id) do nothing",
            &[&U64::from(chain_id)],
        )
        .await
        .wrap_err("inserting chain id into config")?;
    Downloader {
        filter: Filter::new(),
        start,
        pg_pool: pg_pool.clone(),
        eth_client: eth_client.clone(),
        batch_size: args.batch_size,
        concurrency: args.concurrency,
        stop: args.stop_block,
    }
    .run(broadcaster)
    .await;
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

async fn backup(args: ServerArgs) -> Result<()> {
    if args.no_backup {
        tracing::info!("no backups");
        return Ok(());
    }
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        backup::backup(
            &args.pg_url,
            &args.backup_dir,
            &args.backup_bucket,
            args.backup_window,
        )
        .await?;
    }
}

async fn account_limits(config: api::Config) -> Result<()> {
    loop {
        if let Some(limits) = config.gafe.load_account_limits().await {
            *config.account_limits.lock().unwrap() = limits;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn server(args: ServerArgs) {
    let chain_id = ProviderBuilder::new()
        .on_http(args.eth_url.clone())
        .get_chain_id()
        .await
        .expect("unable to request chain_id");
    let config = api::Config {
        chain_id,
        pool: api_ro_pg(&args.pg_url, &args.ro_password),
        broadcaster: api::Broadcaster::new(),
        account_limits: Arc::new(Mutex::new(HashMap::new())),
        free_limit: Arc::new(gafe::AccountLimit::free()),
        open_limit: Arc::new(gafe::AccountLimit::open()),
        gafe: gafe::Connection::new(args.gafe_pg_url.clone(), chain_id).await,
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
             tracing::info_span!(
                 "http", path,
                 status = tracing::field::Empty,
                 "api-key" = tracing::field::Empty,
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
        flatten(tokio::spawn(sync(args.clone(), config.broadcaster.clone()))),
        flatten(tokio::spawn(backup(args.clone()))),
        flatten(tokio::spawn(
            axum::serve(listener, app)
                .into_future()
                .map_err(|e| eyre!("serving http: {}", e))
        )),
    );
    match res {
        Err(err) => panic!("{}", err),
        _ => println!("all done"),
    }
}

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(eyre!("handle error: {}", err)),
    }
}
