use deadpool_postgres::Pool;
use itertools::Itertools;
use std::{cmp, collections::HashMap, fmt, ops::Range, sync::Arc, time::Duration};
use tokio::task::{self, JoinHandle};
use url::Url;

use alloy::{
    hex,
    primitives::{BlockHash, U16, U64},
    providers::{Provider, ProviderBuilder, ReqwestProvider},
    rpc::{
        client::{BatchRequest, RpcClient, Waiter},
        types::eth::{Block, BlockNumberOrTag, Filter, Log},
    },
    transports::http::Http,
};
use eyre::{eyre, Context, OptionExt, Result};
use futures::pin_mut;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};

use crate::api;

/*
    Manager will periodically load config from the database.
    If a config is not running, it will run it.
*/

#[derive(Debug)]
pub enum Error {
    Wait,
    Retry(eyre::Report),
    Fatal(eyre::Report),
}

impl From<eyre::Report> for Error {
    fn from(err: eyre::Report) -> Self {
        Self::Fatal(err)
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
                "select enabled, chain, url, batch_size, concurrency from config",
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
                batch_size: row.get::<&str, U16>("batch_size").to(),
                concurrency: row.get::<&str, U16>("concurrency").to(),
            })
            .collect_vec())
    }
}

pub async fn run(config: api::Config) -> Result<()> {
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
                        Downloader::new(conf, be_pool, api_updates, stat_updates, None)
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
    pub eth_client: ReqwestProvider,
    pub batch_size: u16,
    pub concurrency: u16,
    pub filter: Filter,
    pub start: BlockNumberOrTag,
    stat_updates: Arc<api::Broadcaster2>,
    api_updates: Arc<api::Broadcaster>,
}

impl Downloader {
    pub fn new(
        config: RemoteConfig,
        be_pool: Pool,
        api_updates: Arc<api::Broadcaster>,
        stat_updates: Arc<api::Broadcaster2>,
        start: Option<u64>,
    ) -> Downloader {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        let http_wrapper = Http::with_client(http_client, config.url);
        let rpc_client = RpcClient::new(http_wrapper, false);
        let eth_client = ProviderBuilder::new().on_client(rpc_client);
        let start = match start {
            Some(n) => BlockNumberOrTag::Number(n),
            None => BlockNumberOrTag::Latest,
        };
        Downloader {
            api_updates,
            stat_updates,
            start,
            be_pool,
            chain: config.chain.into(),
            eth_client,
            batch_size: config.batch_size,
            concurrency: config.concurrency,
            filter: Filter::new(),
        }
    }

    #[tracing::instrument(skip_all fields(event, chain = self.chain.0))]
    pub async fn run(&self) {
        self.init_blocks().await.unwrap();
        let mut batch_size = self.batch_size;
        loop {
            match self.download(batch_size).await {
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
                    self.stat_updates
                        .update(api::ChainUpdateSource::Local, self.chain, last);
                    batch_size = self.batch_size
                }
            }
        }
    }

    async fn init_blocks(&self) -> Result<()> {
        if !self
            .be_pool
            .get()
            .await?
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
        tracing::info!("initializing blocks table at: {}", self.start);
        let block = self
            .eth_client
            .get_block_by_number(self.start, alloy::rpc::types::BlockTransactionsKind::Hashes)
            .await?
            .ok_or_eyre(eyre!("missing block {}", self.start))?;
        self.be_pool
            .get()
            .await
            .wrap_err("pg conn")?
            .execute(
                "
                insert into blocks(chain, num, hash)
                values ($1, $2, $3) on conflict(chain, num) do nothing
                ",
                &[
                    &self.chain,
                    &U64::from(block.header.number),
                    &block.header.hash,
                ],
            )
            .await
            .map(|_| ())
            .wrap_err("unable to init blocks table")
    }

    #[tracing::instrument(level="info" skip_all fields(start, end, logs))]
    async fn download(&self, batch_size: u16) -> Result<u64, Error> {
        let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        let next = self.next(&pgtx, batch_size).await?;

        let (start, end, end_hash) = (
            next.start.header.number,
            next.end.header.number,
            next.end.header.hash,
        );
        let filter = self.filter.clone().from_block(start).to_block(end);

        tracing::Span::current()
            .record("start", start)
            .record("end", end);

        let logs = if end - start + 1 >= batch_size as u64 {
            self.batch(batch_size, filter).await?
        } else {
            self.single(filter).await?
        };

        let num_copied = copy(&pgtx, self.chain, logs).await?;
        pgtx.execute(
            "insert into blocks(chain, num, hash) values ($1, $2, $3)",
            &[&self.chain, &U64::from(end), &end_hash],
        )
        .await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        tracing::Span::current().record("logs", num_copied);
        Ok(end)
    }

    #[tracing::instrument(level = "debug" skip_all fields(local, remote))]
    async fn next(&self, pgtx: &Transaction<'_>, batch_size: u16) -> Result<Range<Block>, Error> {
        let mut removed = 0;
        for _ in 0..5000 {
            let latest_remote = self
                .eth_client
                .get_block_by_number(
                    BlockNumberOrTag::Latest,
                    alloy::rpc::types::BlockTransactionsKind::Hashes,
                )
                .await
                .wrap_err("requesting latest block")?
                .ok_or(eyre!("missing latest block"))?;
            let remote_num = latest_remote.header.number;
            let (local_num, local_hash) = self.get_local_latest(pgtx).await?;
            let local_num: u64 = local_num.to();

            self.stat_updates
                .update(api::ChainUpdateSource::Remote, self.chain, remote_num);
            tracing::Span::current()
                .record("local", local_num)
                .record("remote", remote_num);

            if local_num >= remote_num {
                return Err(Error::Wait);
            }

            let mut delta = cmp::min(remote_num - local_num, batch_size as u64);
            if delta < batch_size as u64 {
                delta = 1;
            }
            let (from, to) = (local_num + 1, local_num + delta);
            let (from, to) = (
                self.eth_client
                    .get_block_by_number(
                        BlockNumberOrTag::Number(from),
                        alloy::rpc::types::BlockTransactionsKind::Hashes,
                    )
                    .await
                    .map_err(|e| Error::Retry(eyre!("downloading block: {}", e)))?
                    .ok_or_else(|| Error::Retry(eyre!("missing block: {}", from)))?,
                self.eth_client
                    .get_block_by_number(
                        BlockNumberOrTag::Number(to),
                        alloy::rpc::types::BlockTransactionsKind::Hashes,
                    )
                    .await
                    .map_err(|e| Error::Retry(eyre!("downloading block: {}", e)))?
                    .ok_or_else(|| Error::Retry(eyre!("missing block: {}", to)))?,
            );
            if from.header.parent_hash != local_hash {
                tracing::error!(
                    "reorg remote={}/{} local={}/{} removed={}",
                    from.header.number,
                    hex::encode(&from.header.hash[..4]),
                    local_num,
                    hex::encode(&local_hash[..4]),
                    removed,
                );
                pgtx.execute(
                    "delete from blocks where chain = $1 and num >= $2",
                    &[&self.chain, &U64::from(local_num)],
                )
                .await?;
                pgtx.execute(
                    "delete from logs where chain = $1 and block_num >= $2",
                    &[&self.chain, &U64::from(local_num)],
                )
                .await?;
                removed += 1;
                continue;
            }
            return Ok(from..to);
        }
        return Err(Error::Fatal(eyre!("reorg too deep")));
    }

    #[tracing::instrument(level="debug" skip_all fields(start, end))]
    async fn single(&self, filter: Filter) -> Result<Vec<Log>, Error> {
        let mut batch = BatchRequest::new(self.eth_client.client());
        let block: Waiter<Block> = batch
            .add_call(
                "eth_getBlockByNumber",
                &(U64::from(filter.get_to_block().unwrap()), false),
            )
            .wrap_err("building eth_getBlockByNumber")?;
        let logs: Waiter<Vec<Log>> = batch
            .add_call("eth_getLogs", &(&filter,))
            .wrap_err("building eth_getLogs")?;
        batch.send().await.wrap_err("making batch call")?;
        block.await.map_err(|e| Error::Retry(eyre!("block {e}")))?;
        logs.await.map_err(|e| Error::Retry(eyre!("logs {e}")))
    }

    #[tracing::instrument(level="debug" skip_all fields(start, end))]
    async fn batch(&self, batch_size: u16, filter: Filter) -> Result<Vec<Log>, Error> {
        let part_size = (batch_size / self.concurrency).max(1);
        let mut tasks = Vec::new();
        let (start, end) = (
            filter.get_from_block().unwrap(),
            filter.get_to_block().unwrap(),
        );
        for i in (start..=end).step_by(part_size as usize) {
            let j = (i + part_size as u64 - 1).min(end);
            let r = self.eth_client.clone();
            let f = filter.clone();
            tasks.push(task::spawn(async move {
                r.get_logs(&f.from_block(i).to_block(j))
                    .await
                    .map_err(|e| eyre!("downloading logs {i}:{j} {e}"))
            }))
        }
        let mut logs = vec![];
        for task in tasks {
            logs.extend(
                task.await
                    .wrap_err("async download task")?
                    .map_err(Error::Fatal)?,
            );
        }
        Ok(logs)
    }

    async fn get_local_latest(&self, tx: &Transaction<'_>) -> Result<(U64, BlockHash), Error> {
        let q = "SELECT num, hash from blocks where chain = $1 order by num desc limit 1";
        let row = tx
            .query_one(q, &[&self.chain])
            .await
            .wrap_err("getting local latest")?;
        Ok((row.try_get("num")?, row.try_get("hash")?))
    }
}

#[tracing::instrument(level="debug" fields(logs) skip_all)]
pub async fn copy(pgtx: &Transaction<'_>, chain_id: api::Chain, logs: Vec<Log>) -> Result<u64> {
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
                &chain_id,
                &U64::from(log.block_number.expect("missing block_number")),
                &log.transaction_hash,
                &U64::from(log.log_index.expect("missing log_index")),
                &log.address().0,
                &log.topics(),
                &log.data().data.to_vec(),
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in logs")
}
