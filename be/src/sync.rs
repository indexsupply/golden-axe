use deadpool_postgres::Pool;
use handlebars::{self, Handlebars};
use itertools::Itertools;
use shared::jrpc;
use std::{collections::HashMap, fmt, sync::Arc};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use url::Url;

use alloy::primitives::{BlockHash, FixedBytes, U16, U256, U64};
use eyre::{eyre, Context, Result};
use futures::pin_mut;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};

use crate::{api, broadcast};

#[derive(Debug)]
pub enum Error {
    Wait,
    Retry(String),
    Fatal(eyre::Report),
}

impl From<eyre::Report> for Error {
    fn from(err: eyre::Report) -> Self {
        Self::Fatal(err)
    }
}

impl From<jrpc::Error> for Error {
    fn from(err: jrpc::Error) -> Self {
        if err.message == "no result" {
            Self::Wait
        } else {
            Self::Retry(format!("jrpc error {:?}", err))
        }
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Self::Fatal(eyre!("database-error={}", err.to_string()))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RemoteConfig {
    pub enabled: bool,
    pub chain: u64,
    pub url: Url,
    pub start_block: Option<i64>,
    pub batch_size: u16,
    pub concurrency: u16,
}

impl fmt::Display for RemoteConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteConfig({}, {}, enabled={})",
            self.chain, self.url, self.enabled
        )
    }
}

impl RemoteConfig {
    pub async fn load(pool: &Pool) -> Result<Vec<RemoteConfig>> {
        Ok(pool
            .get()
            .await?
            .query(
                "select enabled, chain, url, start_block, batch_size, concurrency from config",
                &[],
            )
            .await?
            .iter()
            .map(|row| RemoteConfig {
                enabled: row.get("enabled"),
                chain: row.get::<&str, U64>("chain").to(),
                url: row
                    .get::<&str, String>("url")
                    .parse()
                    .expect("unable to parse url"),
                start_block: row.get("start_block"),
                batch_size: row.get::<&str, U16>("batch_size").to(),
                concurrency: row.get::<&str, U16>("concurrency").to(),
            })
            .collect_vec())
    }
}

pub async fn test(url: &str, chain: u64) -> Result<(), shared::Error> {
    let parsed: Url = url.parse().wrap_err("unable to parse rpc url")?;
    let jrpc_client = jrpc::Client::new(parsed.as_str());
    let resp = jrpc_client
        .send_one(serde_json::json!({
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
        }))
        .await;
    match resp {
        Err(e) => Err(shared::Error::User(format!("rpc error {}", e))),
        Ok(resp) => match resp.to::<U64>() {
            Ok(id) if id.to::<u64>() == chain => Ok(()),
            Ok(id) => Err(shared::Error::User(format!(
                "expected chain {} got {}",
                chain, id
            ))),
            Err(e) => Err(shared::Error::User(format!("rpc error {}", e))),
        },
    }
}

pub async fn run(config: api::Config) {
    let mut table: HashMap<RemoteConfig, JoinHandle<()>> = HashMap::new();
    loop {
        let remotes = RemoteConfig::load(&config.fe_pool)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("loading remote config {}", e);
                vec![]
            })
            .into_iter()
            .filter(|rc| rc.enabled)
            .collect_vec();
        for remote in remotes.iter() {
            if !table.contains_key(remote) {
                let (conf, be_pool, broadcaster) = (
                    remote.clone(),
                    config.be_pool.clone(),
                    config.broadcaster.clone(),
                );
                table.insert(
                    conf.clone(),
                    tokio::spawn(
                        async move { Downloader::new(conf, be_pool, broadcaster).run().await },
                    ),
                );
            }
        }
        for key in table.keys().cloned().collect_vec() {
            if let Some(handle) = table.get_mut(&key) {
                if !remotes.iter().any(|rc| rc.eq(&key)) {
                    tracing::error!("aborting {}", key);
                    handle.abort();
                }
                if handle.is_finished() {
                    match handle.await {
                        Ok(_) => tracing::info!("finished {}", key),
                        Err(e) => tracing::error!("{} {:?}", key, e),
                    }
                    table.remove(&key);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub struct Downloader {
    pub chain: api::Chain,
    pub batch_size: u16,
    pub concurrency: u16,
    pub start_block: Option<i64>,

    be_pool: Pool,
    jrpc_client: Arc<jrpc::Client>,
    broadcaster: Arc<broadcast::Channel>,
    partition_max_block: Option<u64>,
}

impl Downloader {
    pub fn new(
        config: RemoteConfig,
        be_pool: Pool,
        broadcaster: Arc<broadcast::Channel>,
    ) -> Downloader {
        let jrpc_client = Arc::new(jrpc::Client::new(config.url.as_ref()));
        Downloader {
            chain: config.chain.into(),
            batch_size: config.batch_size,
            concurrency: config.concurrency,
            start_block: config.start_block,
            be_pool,
            jrpc_client,
            broadcaster,
            partition_max_block: None,
        }
    }

    async fn init_blocks(&mut self) -> Result<(), Error> {
        if !self
            .be_pool
            .get()
            .await
            .wrap_err("getting pg")?
            .query(
                "select true from blocks where chain = $1 limit 1",
                &[&self.chain],
            )
            .await
            .expect("unable to query for latest block")
            .is_empty()
        {
            return Ok(());
        }
        let block = match self.start_block {
            Some(n) => remote_block(&self.jrpc_client, U64::from(n)).await?,
            None => remote_block_latest(&self.jrpc_client).await?,
        };
        tracing::info!("initializing blocks table at: {}", block.number);
        let mut pg = self.be_pool.get().await.wrap_err("getting pg")?;
        let pgtx = pg.transaction().await?;
        self.partition_max_block = setup_tables(
            &pgtx,
            self.chain.0,
            block.number.to(),
            self.partition_max_block,
        )
        .await
        .expect("setting up table for initial block");
        copy_blocks(&pgtx, self.chain, &[block]).await?;
        pgtx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all fields(event, chain = self.chain.0))]
    pub async fn run(mut self) {
        if let Err(e) = self.init_blocks().await {
            tracing::error!("init {:?}", e);
            return;
        }
        let mut batch_size = self.batch_size;
        loop {
            match self.download(batch_size).await {
                Err(Error::Wait) => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(Error::Retry(err)) => {
                    batch_size = std::cmp::max(1, batch_size / 10);
                    tracing::error!("downloading error: {}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(Error::Fatal(err)) => {
                    batch_size = std::cmp::max(1, batch_size / 10);
                    tracing::error!("fatal downloading error: {}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Ok(last) => {
                    self.broadcaster.update(self.chain.0);
                    let _ = self.broadcaster.json_updates.send(serde_json::json!({
                        "new_block": "local",
                        "chain": self.chain.0,
                        "num": last,
                    }));
                    batch_size = self.batch_size
                }
            }
        }
    }

    async fn delete_after(&self, n: u64) -> Result<(), Error> {
        let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        pgtx.execute(
            "delete from blocks where chain = $1 and num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.execute(
            "delete from logs where chain = $1 and block_num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.execute(
            "delete from txs where chain = $1 and block_num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        Ok(())
    }

    /*
    Request remote latest n and local latest k.
    If k == n-1 we simply can download logs for block n-1
    If k <  n-2 we will make n-1-k requests to download n-1-k blocks
    and another request to download n-1-k logs.

    After downloading n-k blocks/logs we check n's parent hash with k's hash.
    If the hashes aren't equal we delete k's block/logs
    and start the process over again.

    If the hashes match, we copy blocks, transactions, and logs into their tables
    */
    #[tracing::instrument(level="info" skip_all, fields(from, to, blocks, txs, logs))]
    async fn download(&mut self, batch_size: u16) -> Result<u64, Error> {
        let latest = remote_block_latest(&self.jrpc_client).await?;
        let _ = self.broadcaster.json_updates.send(serde_json::json!({
            "new_block": "remote",
            "chain": self.chain.0,
            "num": latest.number.to::<u64>(),
        }));
        let (local_num, local_hash) = self.local_latest().await?;
        if local_num >= latest.number.to() {
            return Err(Error::Wait);
        }

        let delta = latest.number.to::<u64>() - local_num;
        let (from, to) = (local_num + 1, local_num + delta.min(batch_size as u64));
        {
            let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
            let pgtx = pg.transaction().await?;
            self.partition_max_block =
                setup_tables(&pgtx, self.chain.0, to, self.partition_max_block).await?;
            pgtx.commit().await.wrap_err("unable to commit tx")?;
        }
        tracing::Span::current()
            .record("from", from)
            .record("to", to);
        let (mut blocks, mut logs) = (
            download_blocks(&self.jrpc_client, from, to).await?,
            download_logs(&self.jrpc_client, from, to).await?,
        );
        add_timestamp(&mut blocks, &mut logs);
        validate_blocks(from, to, &blocks)?;
        validate_logs(&blocks, &logs)?;
        let (first_block, last_block) = (blocks.first().unwrap(), blocks.last().unwrap());

        if first_block.parent_hash != local_hash {
            self.delete_after(local_num).await?;
            return Err(Error::Fatal(eyre!("reorg")));
        }
        let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        let num_logs = copy_logs(&pgtx, self.chain, logs).await?;
        let num_txs = copy_txs(&pgtx, self.chain, &blocks).await?;
        let num_blocks = copy_blocks(&pgtx, self.chain, &blocks).await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        tracing::Span::current()
            .record("blocks", num_blocks)
            .record("logs", num_logs)
            .record("txs", num_txs);
        Ok(last_block.number.to())
    }

    async fn local_latest(&self) -> Result<(u64, BlockHash), Error> {
        let pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let q = "SELECT num, hash from blocks where chain = $1 order by num desc limit 1";
        let row = pg
            .query_one(q, &[&self.chain])
            .await
            .wrap_err("getting local latest")?;
        Ok((
            row.try_get::<&str, i64>("num")? as u64,
            row.try_get("hash")?,
        ))
    }
}

pub async fn sync_one(
    pg: &mut tokio_postgres::Client,
    client: &jrpc::Client,
    chain: u64,
    n: u64,
) -> Result<u64, Error> {
    let mut blocks = download_blocks(client, n, n).await?;
    let mut logs = download_logs(client, n, n).await?;
    add_timestamp(&mut blocks, &mut logs);
    validate_blocks(n, n, &blocks)?;

    let pgtx = pg.transaction().await?;
    let num_logs = copy_logs(&pgtx, api::Chain(chain), logs).await?;
    pgtx.commit().await.wrap_err("unable to commit tx")?;
    Ok(num_logs)
}

async fn remote_block(client: &jrpc::Client, n: U64) -> Result<jrpc::Block, Error> {
    Ok(client
        .send_one(serde_json::json!({
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [n, true],
        }))
        .await?
        .to()?)
}

async fn remote_block_latest(client: &jrpc::Client) -> Result<jrpc::Block, Error> {
    Ok(client
        .send_one(serde_json::json!({
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", true],
        }))
        .await?
        .to()?)
}
#[tracing::instrument(level="info" skip_all, fields(from, to))]
async fn download_blocks(
    client: &jrpc::Client,
    from: u64,
    to: u64,
) -> Result<Vec<jrpc::Block>, Error> {
    Ok(client
        .send(
            (from..=to)
                .map(|n| {
                    serde_json::json!({
                        "id": "1",
                        "jsonrpc": "2.0",
                        "method": "eth_getBlockByNumber",
                        "params": [U64::from(n), true],
                    })
                })
                .collect(),
        )
        .await?
        .into_iter()
        .map(|resp| resp.to::<jrpc::Block>())
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sorted_by(|a, b| a.number.cmp(&b.number))
        .collect())
}

#[tracing::instrument(level="info" skip_all, fields(from, to))]
async fn download_logs(client: &jrpc::Client, from: u64, to: u64) -> Result<Vec<jrpc::Log>, Error> {
    Ok(client
        .send_one(serde_json::json!({
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{"fromBlock": U64::from(from), "toBlock": U64::from(to)}],
        }))
        .await?
        .to()?)
}

fn validate_logs(blocks: &[jrpc::Block], logs: &[jrpc::Log]) -> Result<(), Error> {
    let mut logs_by_block: HashMap<U64, Vec<&jrpc::Log>> = HashMap::new();
    for log in logs {
        logs_by_block.entry(log.block_number).or_default().push(log);
    }
    for block in blocks {
        let has_logs = logs_by_block
            .get(&block.number)
            .map_or(false, |v| !v.is_empty());
        let has_bloom = block.logs_bloom != FixedBytes::<256>::ZERO;
        if !has_logs && has_bloom {
            return Err(Error::Fatal(eyre!("bloom without logs {}", block.number)));
        }
    }
    Ok(())
}

fn validate_blocks(from: u64, to: u64, blocks: &[jrpc::Block]) -> Result<(), Error> {
    if let Some(i) = blocks.first().map(|b| b.number.to::<u64>()) {
        if i != from {
            return Err(Error::Fatal(eyre!("want first block {} got {}", from, i)));
        }
    }
    if let Some(i) = blocks.last().map(|b| b.number.to::<u64>()) {
        if i != to {
            return Err(Error::Fatal(eyre!("want last block {} got {}", from, i)));
        }
    }
    for (prev, curr) in blocks.iter().zip(blocks.iter().skip(1)) {
        if curr.parent_hash != prev.hash {
            return Err(Error::Fatal(eyre!(
                "block {} has wrong parent_hash {} (expected {})",
                curr.number,
                curr.parent_hash,
                prev.hash
            )));
        }
    }
    Ok(())
}

//timescaledb
pub async fn setup_tables(
    pgtx: &Transaction<'_>,
    chain: u64,
    new_block: u64,
    partition_max_block: Option<u64>,
) -> Result<Option<u64>, Error> {
    const N: u64 = 2000000;
    let from = match partition_max_block {
        Some(max) if new_block < max + 1 => return Ok(partition_max_block),
        Some(max) => ((max + 1) / N) * N,
        None => (new_block / N) * N,
    };
    let to = from + N;
    let label = from / 1000000;
    let query = Handlebars::new()
        .render_template(
            "
            create table if not exists blocks_c{{chain}}
            partition of blocks
            for values in ({{chain}})
            partition by range (num);

            create table if not exists blocks_c{{chain}}_b{{label}}
            partition of blocks_c{{chain}}
            for values from ({{from}}) to ({{to}});

            create table if not exists txs_c{{chain}}
            partition of txs
            for values in ({{chain}})
            partition by range (block_num);

            create table if not exists txs_c{{chain}}_b{{label}}
            partition of txs_c{{chain}}
            for values from ({{from}}) to ({{to}});
            alter table txs_c{{chain}}_b{{label}} set (toast_tuple_target = 128);

            create table if not exists logs_c{{chain}}
            partition of logs
            for values in ({{chain}})
            partition by range (block_num);

            create table if not exists logs_c{{chain}}_b{{label}}
            partition of logs_c{{chain}}
            for values from ({{from}}) to ({{to}});
            alter table logs_c{{chain}}_b{{label}} set (toast_tuple_target = 128);
            ",
            &serde_json::json!({"chain": chain, "label": label, "from": from, "to": to,}),
        )
        .wrap_err("rendering sql template")?;
    tracing::info!("new table range label={} from={} to={}", label, from, to);
    pgtx.batch_execute(&query).await?;
    Ok(Some(to - 1))
}

fn add_timestamp(blocks: &mut [jrpc::Block], logs: &mut Vec<jrpc::Log>) {
    for block in blocks.iter_mut() {
        for tx in block.transactions.iter_mut() {
            tx.block_timestamp = Some(block.timestamp);
        }
    }
    let indexed: HashMap<u64, &jrpc::Block> = blocks.iter().map(|b| (b.number.to(), b)).collect();
    for log in logs {
        if log.block_timestamp.is_none() {
            if let Some(block) = indexed.get(&log.block_number.to()) {
                log.block_timestamp = Some(block.timestamp);
            }
        }
    }
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_logs(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    logs: Vec<jrpc::Log>,
) -> Result<u64> {
    const Q: &str = "
        copy logs (
            chain,
            block_num,
            block_timestamp,
            log_idx,
            tx_hash,
            address,
            topics,
            data
        )
        from stdin binary
    ";
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TIMESTAMPTZ,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA_ARRAY,
            tokio_postgres::types::Type::BYTEA,
        ],
    );
    pin_mut!(writer);
    for log in logs {
        writer
            .as_mut()
            .write(&[
                &chain,
                &log.block_number,
                &OffsetDateTime::from_unix_timestamp(
                    log.block_timestamp.expect("missing log ts").to::<u64>() as i64,
                )?,
                &log.log_idx,
                &log.tx_hash,
                &log.address,
                &log.topics,
                &log.data.to_vec(),
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in logs")
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_txs(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    blocks: &[jrpc::Block],
) -> Result<u64> {
    const Q: &str = r#"
        copy txs (
            chain,
            block_num,
            block_timestamp,
            idx,
            type,
            gas,
            gas_price,
            hash,
            nonce,
            "from",
            "to",
            input,
            value
        )
        from stdin binary
    "#;
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TIMESTAMPTZ,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::INT2,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::NUMERIC,
        ],
    );
    pin_mut!(writer);
    for block in blocks {
        for tx in &block.transactions {
            writer
                .as_mut()
                .write(&[
                    &chain,
                    &block.number,
                    &OffsetDateTime::from_unix_timestamp(
                        tx.block_timestamp.expect("missing tx ts").to::<u64>() as i64,
                    )?,
                    &tx.idx,
                    &tx.ty.unwrap_or(U64::from(0)),
                    &tx.gas,
                    &tx.gas_price.unwrap_or(U256::from(0)),
                    &tx.hash,
                    &tx.nonce,
                    &tx.from.to_vec(),
                    &tx.to.unwrap_or_default().to_vec(),
                    &tx.input.to_vec(),
                    &tx.value,
                ])
                .await?;
        }
    }
    writer.finish().await.wrap_err("unable to copy in txs")
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_blocks(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    blocks: &[jrpc::Block],
) -> Result<u64> {
    const Q: &str = r#"
        copy blocks (
            chain,
            num,
            timestamp,
            gas_limit,
            gas_used,
            hash,
            nonce,
            receipts_root,
            state_root,
            extra_data,
            miner
        )
        from stdin binary
    "#;
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TIMESTAMPTZ,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
        ],
    );
    pin_mut!(writer);
    for block in blocks {
        writer
            .as_mut()
            .write(&[
                &chain,
                &block.number,
                &OffsetDateTime::from_unix_timestamp(block.timestamp.to())?,
                &block.gas_limit,
                &block.gas_used,
                &block.hash,
                &block.nonce,
                &block.receipts_root,
                &block.state_root,
                &block.extra_data.to_vec(),
                &block.miner.to_vec(),
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in blocks")
}
