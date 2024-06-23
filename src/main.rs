mod sync;
mod view;

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::rpc::types::eth::BlockNumberOrTag;
use alloy::{json_abi::Event, rpc::types::eth::Filter};
use clap::{Parser, Subcommand};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{eyre, Context, Result};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use sync::Downloader;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Subcommand)]
enum Commands {
    Sync,
    PrintView,
}

#[derive(Parser, Debug)]
#[command(name = "ga", about = "GOLDEN AXE: eth logs indexer", version = "0.1")]
struct Args {
    #[arg(short, long, global = true, env = "ADDRESS")]
    address: Option<Vec<Address>>,

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

    #[arg(
        long = "eth",
        env = "ETH_URL",
        default_value = "https://base-rpc.publicnode.com"
    )]
    eth_url: Url,

    #[arg(short, long, global = true, help = "human readable abi signature")]
    event: Option<String>,

    #[arg(
        long,
        short = 'f',
        global = true,
        help = "newline separated, human-readable abi signatures"
    )]
    events_file: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}
static SCHEMA: &'static str = include_str!("./schema.sql");

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let args = Args::parse();
    match args.command {
        Some(Commands::PrintView) => print_view(&args)?,
        Some(Commands::Sync) | None => sync(&args).await?,
    }
    Ok(())
}

fn print_view(args: &Args) -> Result<()> {
    for event in parse_events(&args)? {
        println!("{}", view::fmt_sql(&view::create(&event)?)?);
    }
    Ok(())
}

fn parse_events(args: &Args) -> Result<Vec<Event>> {
    let mut events: Vec<Event> = Vec::new();
    if let Some(path) = &args.events_file {
        let path = Path::new(&path);
        let file = File::open(&path)?;
        let reader = io::BufReader::new(file);
        for line in reader.lines() {
            let data = line?;
            events.push(Event::parse(&data).wrap_err(format!("unable to abi parse: {}", data))?);
        }
    }
    if let Some(event) = &args.event {
        events.push(
            event
                .parse()
                .wrap_err(eyre!("unable to abi parse: {}", &event))?,
        );
    }
    if events.len() == 0 {
        return Err(eyre!("must provide 1 event via -a or -f"));
    }
    Ok(events)
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

async fn sync(args: &Args) -> Result<()> {
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

    let events = parse_events(args)?;
    {
        for event in events {
            tracing::info!("indexing: {:x}", event.selector());
            tracing::info!("indexing: {}", event.signature());
            let view = view::create(&event)?;
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
                event,
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
    tokio::signal::ctrl_c().await.expect("handling exit signal");
    Ok(())
}
