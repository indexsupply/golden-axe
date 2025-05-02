use alloy::primitives::U64;
use clap::Parser;
use shared::{jrpc, pg};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};

#[derive(Parser)]
struct Args {
    #[arg(long = "pg", env = "PG_URL", default_value = "postgres://localhost/be")]
    pg_url: String,
    #[arg(long = "rpc", env = "RPC_URL")]
    rpc_url: String,
    #[arg(long = "block")]
    block: Option<u64>,
    #[arg(long = "input")]
    input: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let pool = pg::new_pool(&args.pg_url, 1).expect("unable to create pg pool");
    let mut pg = pool.get().await.expect("unable to get pg from pool");

    let client = jrpc::Client::new(&args.rpc_url);
    let chain = client.chain_id().await.expect("getting chain id");

    let blocks = if let Some(file) = args.input {
        read_blocks_from_file(&file).await
    } else if let Some(block) = args.block {
        vec![block]
    } else {
        panic!("must provide either --block or --input");
    };

    for block in blocks {
        sync_if_missing(&mut pg, &client, chain, block).await;
    }
}

async fn read_blocks_from_file(path: &str) -> Vec<u64> {
    let file = File::open(path).await.expect("failed to open input file");
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut blocks = Vec::new();

    while let Some(line) = lines.next_line().await.expect("failed to read line") {
        let block = line.trim().parse().expect("invalid block number");
        blocks.push(block);
    }

    blocks
}

async fn sync_if_missing(
    pg: &mut tokio_postgres::Client,
    client: &jrpc::Client,
    chain: u64,
    block: u64,
) {
    let count: i64 = pg
        .query_one(
            "select count(*) from logs where chain = $1 and block_num = $2",
            &[&U64::from(chain), &U64::from(block)],
        )
        .await
        .expect("query failed")
        .get(0);

    if count == 0 {
        let num_logs = be::sync::sync_one(pg, client, chain, block)
            .await
            .expect("sync failed");
        println!("downloaded {} logs", num_logs);
    } else {
        println!("nothing to do");
    }
}
