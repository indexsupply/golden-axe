use alloy::primitives::U64;
use clap::Parser;
use shared::{jrpc, pg};

#[derive(Parser)]
struct Args {
    #[arg(long = "pg", env = "PG_URL", default_value = "postgres://localhost/be")]
    pg_url: String,
    #[arg(long = "rpc", env = "RPC_URL")]
    rpc_url: String,
    #[arg(long = "range")]
    range: u64,
    #[clap(short = 'd', action = clap::ArgAction::SetTrue)]
    download: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let pool = pg::new_pool(&args.pg_url, 1).expect("unable to create pg pool");
    let mut pg = pool.get().await.expect("unable to get pg from pool");

    let client = jrpc::Client::new(&args.rpc_url);
    let chain = client.chain_id().await.expect("getting chain id");

    let blocks = find_missing(&pg, chain, args.range)
        .await
        .expect("finding missing logs");
    if blocks.is_empty() {
        println!("no missing logs");
    }
    for block in blocks {
        println!("missing {} txs: {}", block.num, block.txs);
        if args.download {
            sync_if_missing(&mut pg, &client, chain, block.num).await;
        }
    }
}

async fn sync_if_missing(
    pg: &mut tokio_postgres::Client,
    client: &jrpc::Client,
    chain: u64,
    block: i64,
) {
    let count: i64 = pg
        .query_one(
            "select count(*) from logs where chain = $1 and block_num = $2",
            &[&U64::from(chain), &block],
        )
        .await
        .expect("query failed")
        .get(0);

    if count == 0 {
        let num_logs = be::sync::sync_one(pg, client, chain, block as u64)
            .await
            .expect("sync failed");
        println!("downloaded {} logs", num_logs);
    } else {
        println!("nothing to do");
    }
}

fn table_name(tbl: &str, chain: u64, range: u64) -> String {
    format!("{}_c{}_b{}", tbl, chain, range)
}

struct Missing {
    num: i64,
    txs: i64,
}

async fn find_missing(
    pg: &tokio_postgres::Client,
    chain: u64,
    range: u64,
) -> Result<Vec<Missing>, shared::Error> {
    let (from, to) = ((range * 1000000), (range + 2 * 1000000));
    Ok(pg
        .query(
            &format!(
                "
                select b, t.tx_count
                from generate_series($1::int8, $2::int8) as b
                left join (
                    select block_num from {}
                    group by block_num
                ) l on b = l.block_num
                left join (
                    select block_num, count(*) as tx_count from {}
                    group by block_num having count(*) > 1
                ) t on b = t.block_num
                where l.block_num is null and t.block_num is not null
                ",
                table_name("logs", chain, range),
                table_name("txs", chain, range)
            ),
            &[&U64::from(from), &U64::from(to)],
        )
        .await?
        .iter()
        .map(|row| Missing {
            num: row.get("b"),
            txs: row.get("tx_count"),
        })
        .collect())
}
