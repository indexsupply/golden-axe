mod api;
mod api_sql;
mod sql_generate;
mod sql_validate;
mod sync;

use std::time::Duration;

use alloy::{
    primitives::Address,
    providers::ProviderBuilder,
    rpc::types::eth::{BlockNumberOrTag, Filter},
};
use axum::routing::{get, post, Router};
use clap::{Parser, Subcommand};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{Context, Result};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::str::FromStr;
use sync::Downloader;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, timeout::TimeoutLayer};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(name = "server", about = "Serve API requests for decoded logs")]
    Server(ServerArgs),

    #[command(name = "sync", about = "Sync blocks from the API to PG")]
    Sync(ServerArgs),

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
    #[arg(from_global)]
    address: Option<Vec<Address>>,

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
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let args = GlobalArgs::parse();
    match args.command {
        Some(Commands::PrintView(args)) => api_sql::cli::print_view(&args)?,
        Some(Commands::Query(args)) => api_sql::cli::request(&reqwest::Client::new(), args).await?,
        Some(Commands::Sync(args)) => sync(args).await?,
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

async fn sync(args: ServerArgs) -> Result<()> {
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
                tokio::spawn(async move { dl.run().await });
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
            tokio::spawn(async move { dl.run().await });
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
    let config = api::Config {
        pool: api_ro_pg(&args.pg_url, &args.ro_password),
    };
    let service = tower::ServiceBuilder::new()
        .layer(TimeoutLayer::new(Duration::from_secs(10)))
        .layer(CompressionLayer::new());
    let app = Router::new()
        .route("/", get(|| async { "hello\n" }))
        .route("/query", post(api_sql::handle))
        .layer(service)
        .layer(CorsLayer::permissive())
        .with_state(config.clone());
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .expect("binding to tcp for http server");

    tokio::spawn(sync(args));
    axum::serve(listener, app).await.wrap_err("serving http")
}
