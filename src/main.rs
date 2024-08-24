mod api;
mod api_sql;
mod backup;
mod s256;
mod sql_generate;
mod sql_test;
mod sql_validate;
mod sync;

use std::{future::ready, sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, U64},
    providers::{Provider, ProviderBuilder},
    rpc::types::eth::{BlockNumberOrTag, Filter},
};
use axum::{
    body::Body,
    extract::MatchedPath,
    routing::{get, post, Router},
};
use clap::{Parser, Subcommand};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::str::FromStr;
use sync::Downloader;
use tower_http::{
    classify::ServerErrorsFailureClass, compression::CompressionLayer, cors::CorsLayer,
    timeout::TimeoutLayer, trace::TraceLayer,
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

    #[command(name = "view", about = "Print SQL VIEW for events")]
    PrintView(api_sql::cli::Request),

    #[command(name = "query", about = "Query decoded logs", long_about = Some(api_sql::cli::HELP))]
    Query(api_sql::cli::Request),
}

#[derive(Parser)]
#[command(name = "ga", about = "The final indexer", version = "0.1")]
struct GlobalArgs {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, global = true, env = "ADDRESS")]
    address: Option<Vec<Address>>,

    #[arg(short, long, global = true, help = "human readable abi signature")]
    event: Option<String>,

    #[arg(
        long,
        short = 'f',
        env = "EVENTS_FILE",
        global = true,
        help = "newline separated, human-readable abi signatures"
    )]
    events_file: Option<String>,

    #[clap(
        long = "url",
        global = true,
        env = "GA_URL",
        help = "url to golden axe http api",
        default_value = "http://golden-axe-1:8000"
    )]
    url: Url,
}

#[derive(Parser, Debug)]
struct ServerArgs {
    #[arg(long, default_value = "false")]
    skip_migrations: bool,

    #[arg(from_global)]
    address: Option<Vec<Address>>,

    #[command(flatten)]
    backup: backup::Args,

    #[arg(from_global)]
    events_file: Option<String>,

    #[arg(from_global)]
    event: Option<String>,

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
}
static SCHEMA: &str = include_str!("./schema.sql");

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
        Some(Commands::Backup(args)) => backup::run(&args.pg_url, &args.backup).await?,
        Some(Commands::Restore(args)) => backup::restore(&args.pg_url, &args.backup).await?,
        Some(Commands::PrintView(args)) => api_sql::cli::print_view(&args)?,
        Some(Commands::Query(args)) => api_sql::cli::request(&reqwest::Client::new(), args).await?,
        Some(Commands::Server(args)) => server(args).await?,
        None => server(ServerArgs::parse()).await?,
    }
    Ok(())
}

fn log_filter(filter: &Filter) {
    let addresses: Vec<String> = filter.address.iter().map(|a| a.to_string()).collect();
    let topics: Vec<Vec<String>> = filter
        .topics
        .iter()
        .map(|ts| ts.iter().map(|t| t.to_string()).collect::<Vec<String>>())
        .collect();
    tracing::info!("addresses={:?}", addresses);
    tracing::info!("topics={:?}", topics);
}

async fn sync(args: ServerArgs, broadcaster: Arc<api::Broadcaster>) -> Result<()> {
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

    if !args.skip_migrations {
        pg_pool
            .get()
            .await
            .wrap_err("getting pg conn from pool")?
            .batch_execute(SCHEMA)
            .await
            .unwrap();
    }

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

    match api_sql::cli::parse_events(&args.events_file, &args.event)? {
        Some(events) => {
            for event in events {
                tracing::info!("indexing: {:x}", event.selector());
                tracing::info!("indexing: {}", event.signature());
                let view = sql_generate::view(&event)?;
                pg_pool
                    .get()
                    .await
                    .expect("getting conn from pool")
                    .execute(&view, &[])
                    .await?;
                tracing::info!("create-view: {}", event.name.to_lowercase());
                let addrs = args.address.clone().unwrap_or_default();
                let filter = Filter::new().event(&event.signature()).address(addrs);
                log_filter(&filter);
                let dl = Downloader {
                    filter,
                    start,
                    pg_pool: pg_pool.clone(),
                    eth_client: eth_client.clone(),
                    batch_size: args.batch_size,
                    concurrency: args.concurrency,
                    stop: args.stop_block,
                };
                let broadcaster = broadcaster.clone();
                tokio::spawn(async move { dl.run(broadcaster).await });
            }
        }
        None => {
            tracing::info!("indexing all logs");
            let dl = Downloader {
                filter: Filter::new(),
                start,
                pg_pool: pg_pool.clone(),
                eth_client: eth_client.clone(),
                batch_size: args.batch_size,
                concurrency: args.concurrency,
                stop: args.stop_block,
            };
            tokio::spawn(async move { dl.run(broadcaster).await });
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

async fn server(args: ServerArgs) -> Result<()> {
    let config = Arc::new(api::Config {
        pool: api_ro_pg(&args.pg_url, &args.ro_password),
        broadcaster: api::Broadcaster::new(),
    });

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
            tracing::info_span!("http", path, status = tracing::field::Empty)
        })
        .on_response(
            |resp: &axum::http::Response<_>, d: Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_str());
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
    let service = tower::ServiceBuilder::new()
        .layer(tracing)
        .layer(TimeoutLayer::new(Duration::from_secs(10)))
        .layer(CompressionLayer::new());

    let app = Router::new()
        .route("/", get(|| async { "hello\n" }))
        .route("/metrics", get(move || ready(prom_handler.render())))
        .route("/query", get(api_sql::handle))
        .route("/query", post(api_sql::handle_json))
        .route("/query-live", get(api_sql::handle_sse))
        .layer(service)
        .layer(CorsLayer::permissive())
        .with_state(config.clone());
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .expect("binding to tcp for http server");

    tokio::spawn(sync(args, config.broadcaster.clone()));
    axum::serve(listener, app).await.wrap_err("serving http")
}
