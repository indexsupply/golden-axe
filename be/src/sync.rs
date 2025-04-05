use deadpool_postgres::Pool;
use itertools::Itertools;
use shared::jrpc;
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
use tokio::task::{self, JoinHandle};
use url::Url;

use alloy::primitives::{BlockHash, U16, U64};
use eyre::{eyre, Context, Result};
use futures::pin_mut;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};

use crate::api;

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
                let (conf, be_pool, api_updates, stat_updates) = (
                    remote.clone(),
                    config.be_pool.clone(),
                    config.api_updates.clone(),
                    config.stat_updates.clone(),
                );
                table.insert(
                    conf.clone(),
                    tokio::spawn(async move {
                        Downloader::new(conf, be_pool, api_updates, stat_updates)
                            .run()
                            .await
                    }),
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
    pub be_pool: Pool,
    pub jrpc_client: Arc<jrpc::Client>,
    pub batch_size: u16,
    pub concurrency: u16,
    stat_updates: Arc<api::JsonBroadcaster>,
    pub start_block: Option<i64>,
    api_updates: Arc<api::Broadcaster>,
}

impl Downloader {
    pub fn new(
        config: RemoteConfig,
        be_pool: Pool,
        api_updates: Arc<api::Broadcaster>,
        stat_updates: Arc<api::JsonBroadcaster>,
    ) -> Downloader {
        let jrpc_client = Arc::new(jrpc::Client::new(config.url.as_ref()));
        let start_block = config.start_block;
        Downloader {
            api_updates,
            stat_updates,
            start_block,
            be_pool,
            chain: config.chain.into(),
            jrpc_client,
            batch_size: config.batch_size,
            concurrency: config.concurrency,
        }
    }

    async fn init_blocks(&self) -> Result<(), Error> {
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
            Some(n) => self.remote_block(U64::from(n)).await?,
            None => self.remote_block_latest().await?,
        };
        tracing::info!("initializing blocks table at: {}", block.number);
        let mut pg = self.be_pool.get().await.wrap_err("getting pg")?;
        let pgtx = pg.transaction().await?;
        pgtx.execute(
            "
            insert into blocks(chain, num, hash)
            values ($1, $2, $3) on conflict(chain, num) do nothing
            ",
            &[&self.chain, &block.number, &block.hash],
        )
        .await?;
        let stmt = format!(
            r#"create table if not exists "logs_{}" partition of logs for values in ({})"#,
            self.chain.0, self.chain.0
        );
        pgtx.execute(&stmt, &[]).await?;
        Ok(pgtx.commit().await.map(|_| ()).wrap_err("committing tx")?)
    }

    #[tracing::instrument(skip_all fields(event, chain = self.chain.0))]
    pub async fn run(&self) {
        if let Err(e) = self.init_blocks().await {
            tracing::error!("init {:?}", e);
            return;
        }
        let mut batch_size = self.batch_size;
        loop {
            let latest = match self.remote_block_latest().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("getting latest {:?}", e);
                    return;
                }
            };
            match self.download(batch_size, latest).await {
                Err(Error::Wait) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(Error::Retry(err)) => {
                    tracing::error!("downloading error: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(Error::Fatal(err)) => {
                    batch_size = std::cmp::max(1, batch_size / 10);
                    tracing::error!("downloading error: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(last) => {
                    self.api_updates.broadcast(self.chain, last);
                    self.stat_updates.update(serde_json::json!({
                        "new_block": "local",
                        "chain": self.chain,
                        "num": last,
                    }));
                    batch_size = self.batch_size
                }
            }
        }
    }

    #[tracing::instrument(level="info" skip_all, fields(from, to, parts, blocks, logs))]
    async fn download(&self, batch_size: u16, latest: jrpc::Block) -> Result<u64, Error> {
        let (local_num, _) = self.local_latest().await?;
        let remote_num = latest.number.to::<u64>();
        if local_num >= remote_num {
            return Err(Error::Wait);
        }
        let delta = remote_num - local_num;
        let from = local_num + 1;
        let to = local_num + delta.min(batch_size as u64);
        let part_size = (batch_size / self.concurrency).max(1);
        tracing::Span::current()
            .record("from", from)
            .record("to", to)
            .record("blocks", to - from + 1)
            .record("parts", part_size);
        let mut tasks = Vec::new();
        for i in (from..=to).step_by(part_size as usize) {
            let j = (i + part_size as u64 - 1).min(to);
            let jc = self.jrpc_client.clone();
            tasks.push(task::spawn(async move {
                jc.send_one(serde_json::json!({
                    "id": "1",
                    "jsonrpc": "2.0",
                    "method": "eth_getLogs",
                    "params": [{"fromBlock": U64::from(i), "toBlock": U64::from(j)}],
                }))
                .await
            }))
        }
        let mut logs: Vec<jrpc::Log> = vec![];
        for task in tasks {
            let resp = task.await.expect("waiting on task")?;
            logs.extend(resp.to::<Vec<jrpc::Log>>()?);
        }
        let last_block = if latest.number.to::<u64>() != to {
            self.remote_block(U64::from(to)).await?
        } else {
            latest
        };
        let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        let num_copied = copy(&pgtx, self.chain, logs).await?;
        pgtx.execute(
            "insert into blocks(chain, num, hash) values ($1, $2, $3)",
            &[&self.chain, &last_block.number, &last_block.hash],
        )
        .await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        tracing::Span::current().record("logs", num_copied);
        Ok(last_block.number.to())
    }

    #[tracing::instrument(level="info" skip_all fields(n))]
    async fn delta(&self) -> Result<u64, Error> {
        let (local_num, _) = self.local_latest().await?;
        let remote = self.remote_block_latest().await?;
        let delta = remote.number.to::<u64>() - local_num;
        tracing::Span::current().record("δ", delta);
        Ok(delta)
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
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        Ok(())
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

    async fn remote_block(&self, n: U64) -> Result<jrpc::Block, Error> {
        Ok(self
            .jrpc_client
            .send_one(serde_json::json!({
                "id": "1",
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [n, true],
            }))
            .await?
            .to()?)
    }

    async fn remote_block_latest(&self) -> Result<jrpc::Block, Error> {
        Ok(self
            .jrpc_client
            .send_one(serde_json::json!({
                "id": "1",
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ["latest", true],
            }))
            .await?
            .to()?)
    }

    async fn remote_block_with_logs(&self, n: U64) -> Result<(jrpc::Block, Vec<jrpc::Log>), Error> {
        let res = self
            .jrpc_client
            .send(serde_json::json!([{
                "id": "1",
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [n, true],
            }, {
                "id": "2",
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [{"fromBlock": n, "toBlock": n}],
            }]))
            .await?;
        if res.len() != 2 {
            return Err(Error::Retry(String::from(
                "expected 2 results from batch rpc call",
            )));
        }
        let mut iter = res.into_iter();
        let block: jrpc::Block = iter.next().unwrap().to()?;
        let logs: Vec<jrpc::Log> = iter.next().unwrap().to()?;
        Ok((block, logs))
    }
}

#[tracing::instrument(level="debug" fields(chain, logs) skip_all)]
pub async fn copy(pgtx: &Transaction<'_>, chain: api::Chain, logs: Vec<jrpc::Log>) -> Result<u64> {
    const Q: &str = "
        copy logs (
            chain,
            block_num,
            tx_hash,
            log_idx,
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
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::INT4,
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
                &log.tx_hash,
                &log.log_idx,
                &log.address,
                &log.topics,
                &log.data.to_vec(),
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in logs")
}
