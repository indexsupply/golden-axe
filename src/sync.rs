use deadpool_postgres::Pool;
use std::{cmp, ops::Range, sync::Arc, time::Duration};
use tokio::task;

use alloy::{
    hex,
    primitives::{BlockHash, FixedBytes, U64},
    providers::{Provider, ReqwestProvider},
    rpc::{
        client::{BatchRequest, Waiter},
        types::{
            eth::{Block, BlockNumberOrTag, Filter, Log},
            ValueOrArray,
        },
    },
};
use eyre::{eyre, Context, OptionExt, Result};
use futures::pin_mut;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};

use crate::api;

#[derive(Debug)]
pub enum Error {
    Retry(eyre::Report),
    Fatal(eyre::Report),
    Done,
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

pub struct Downloader {
    pub pg_pool: Pool,
    pub eth_client: ReqwestProvider,
    pub batch_size: u64,
    pub concurrency: u64,
    pub filter: Filter,
    pub start: BlockNumberOrTag,
    pub stop: Option<u64>,
}

impl Downloader {
    #[tracing::instrument(skip_all fields(event))]
    pub async fn run(&self, broadcaster: Arc<api::Broadcaster>) {
        {
            let pg = self
                .pg_pool
                .get()
                .await
                .expect("unable to get pg from pool");
            if pg
                .query(
                    "select true from blocks where topic = $1 limit 1",
                    &[&self.topic()],
                )
                .await
                .expect("unable to query for latest block")
                .is_empty()
            {
                self.init_blocks().await.unwrap()
            }
        }
        let mut batch_size = self.batch_size;
        loop {
            match self.download(batch_size).await {
                Err(Error::Retry(err)) => {
                    tracing::debug!("downloading error: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(Error::Fatal(err)) => {
                    batch_size = std::cmp::max(1, batch_size / 10);
                    tracing::error!("downloading error: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(Error::Done) => {
                    println!("all done");
                    return;
                }
                Ok(last) => {
                    broadcaster.broadcast(last);
                    batch_size = self.batch_size
                }
            }
        }
    }

    async fn init_blocks(&self) -> Result<()> {
        tracing::info!("initializing blocks table at: {}", self.start);
        let block = self
            .eth_client
            .get_block_by_number(self.start, false)
            .await?
            .ok_or_eyre(eyre!("missing block {}", self.start))?;
        self.pg_pool
            .get()
            .await
            .wrap_err("pg conn")?
            .execute(
                "
                insert into blocks(num, hash, topic)
                values ($1, $2, $3) on conflict(num, topic) do nothing
                ",
                &[
                    &U64::from(block.header.number.expect("missing header number")),
                    &block.header.hash.unwrap_or_default(),
                    &self.topic(),
                ],
            )
            .await
            .map(|_| ())
            .wrap_err("unable to init blocks table")
    }

    #[tracing::instrument(level="info" skip_all fields(start, end, logs))]
    async fn download(&self, batch_size: u64) -> Result<u64, Error> {
        let mut pg = self.pg_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        let next = self.next(&pgtx, batch_size).await?;

        let (start, end, end_hash) = (
            next.start.header.number.unwrap(),
            next.end.header.number.unwrap(),
            next.end.header.hash.unwrap(),
        );
        let filter = self.filter.clone().select(start..end);

        tracing::Span::current()
            .record("start", start)
            .record("end", end);

        let logs = if end - start + 1 >= batch_size {
            self.batch(batch_size, filter).await?
        } else {
            self.single(filter).await?
        };

        let num_copied = copy(&pgtx, logs).await?;
        pgtx.execute(
            "insert into blocks(num, hash, topic) values ($1, $2, $3)",
            &[&U64::from(end), &end_hash, &self.topic()],
        )
        .await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        tracing::Span::current().record("logs", num_copied);
        Ok(end)
    }

    #[tracing::instrument(level = "debug" skip_all fields(local, remote))]
    async fn next(&self, pgtx: &Transaction<'_>, batch_size: u64) -> Result<Range<Block>, Error> {
        let mut removed = 0;
        for _ in 0..100 {
            let latest_remote = self
                .eth_client
                .get_block_by_number(BlockNumberOrTag::Latest, false)
                .await
                .wrap_err("requesting latest block")?
                .ok_or(eyre!("missing latest block"))?;
            let mut remote_num = latest_remote.header.number.unwrap();
            let (local_num, local_hash) = self.get_local_latest(pgtx).await?;
            let local_num: u64 = local_num.to();

            tracing::Span::current()
                .record("local", local_num)
                .record("remote", remote_num);

            if local_num >= remote_num {
                return Err(Error::Retry(eyre!("nothing new")));
            }

            if let Some(n) = self.stop {
                if local_num >= n {
                    return Err(Error::Done);
                }
                if remote_num > n {
                    remote_num = n;
                }
            }

            let mut delta = cmp::min(remote_num - local_num, batch_size);
            if delta < batch_size {
                delta = 1;
            }
            let (from, to) = (local_num + 1, local_num + delta);
            let (from, to) = (
                self.eth_client
                    .get_block_by_number(BlockNumberOrTag::Number(from), false)
                    .await
                    .map_err(|e| Error::Retry(eyre!("downloading block: {}", e)))?
                    .ok_or_else(|| Error::Retry(eyre!("missing block: {}", from)))?,
                self.eth_client
                    .get_block_by_number(BlockNumberOrTag::Number(to), false)
                    .await
                    .map_err(|e| Error::Retry(eyre!("downloading block: {}", e)))?
                    .ok_or_else(|| Error::Retry(eyre!("missing block: {}", to)))?,
            );
            if from.header.parent_hash != local_hash {
                tracing::error!(
                    "reorg remote={}/{} local={}/{} removed={}",
                    from.header.number.unwrap(),
                    hex::encode(&from.header.hash.unwrap()[..4]),
                    local_num,
                    hex::encode(&local_hash[..4]),
                    removed,
                );
                pgtx.execute(
                    "delete from blocks where num >= $1 and topic = $2",
                    &[&U64::from(local_num), &self.topic()],
                )
                .await?;
                pgtx.execute(
                    "delete from logs where block_num >= $1 and topics[1] = $2",
                    &[&U64::from(local_num), &self.topic()],
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
    async fn batch(&self, batch_size: u64, filter: Filter) -> Result<Vec<Log>, Error> {
        let part_size = (batch_size / self.concurrency).max(1);
        let mut tasks = Vec::new();
        let (start, end) = (
            filter.get_from_block().unwrap(),
            filter.get_to_block().unwrap(),
        );
        for i in (start..=end).step_by(part_size as usize) {
            let j = (i + part_size - 1).min(end);
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

    fn topic(&self) -> FixedBytes<32> {
        if let Some(topics) = self.filter.topics.first() {
            if let Some(ValueOrArray::Value(topic)) = topics.to_value_or_array() {
                return topic;
            }
        }
        FixedBytes::<32>::ZERO
    }

    async fn get_local_latest(&self, tx: &Transaction<'_>) -> Result<(U64, BlockHash), Error> {
        let q = "SELECT num, hash from blocks where topic = $1 order by num desc limit 1";
        let row = tx
            .query_one(q, &[&self.topic()])
            .await
            .wrap_err("getting local latest")?;
        Ok((row.try_get("num")?, row.try_get("hash")?))
    }
}

#[tracing::instrument(level="debug" fields(logs) skip_all)]
async fn copy(pgtx: &Transaction<'_>, logs: Vec<Log>) -> Result<u64> {
    tracing::Span::current().record("logs", logs.len());
    const Q: &str = "
        copy logs (
            block_num,
            tx_hash,
            log_idx,
            address,
            topics,
            data
        )
        from stdin binary
    ";
    let sink = pgtx.copy_in(Q).await.wrap_err("unable to start copy in")?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::NUMERIC,
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
