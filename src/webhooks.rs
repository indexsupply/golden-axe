use core::time;
use std::sync::Arc;

use alloy::primitives::U64;
use deadpool_postgres::Pool;
use eyre::Result;

use crate::{
    api,
    api_sql::{self, Request},
};

async fn load_jobs(pool: &Pool) -> Result<Vec<api_sql::Request>> {
    Ok(pool
        .get()
        .await?
        .query(
            "select
                    destination,
                    block_height,
                    api_key,
                    chain,
                    event_signatures,
                    query
                from webhooks
                ",
            &[],
        )
        .await?
        .iter()
        .map(|row| Request {
            destination: row.get("destination"),
            block_height: Some(row.get::<&str, U64>("block_height").to()),
            api_key: Some(api::Key(row.get::<&str, String>("api_key"))),
            chain: Some(api::Chain(row.get::<&str, U64>("chain").to())),
            event_signatures: row.get("event_signatures"),
            query: row.get("query"),
        })
        .collect())
}

pub async fn run(pool: Pool, broadcaster: Arc<api::Broadcaster>) -> Result<()> {
    let http_client = reqwest::Client::builder().build().unwrap();
    let requests = load_jobs(&pool).await.expect("loading jobs");
    for req in requests {
        tokio::spawn(handle(
            pool.clone(),
            http_client.clone(),
            broadcaster.clone(),
            req,
        ));
    }
    Ok(())
}

#[tracing::instrument(skip_all fields(dest))]
async fn handle(
    pool: Pool,
    http: reqwest::Client,
    broadcaster: Arc<api::Broadcaster>,
    mut req: Request,
) {
    tracing::Span::current().record("dest", req.destination.as_ref().unwrap());
    let mut rx = broadcaster.wait(req.chain.unwrap());
    loop {
        match rx.recv().await {
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!("closed");
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                tracing::info!("lagged");
            }
            Ok(_) => deliver(&http, &pool, &mut req).await,
        }
    }
}

#[tracing::instrument(skip_all fields(status, block))]
async fn deliver(http: &reqwest::Client, pool: &Pool, req: &mut api_sql::Request) {
    if let Some(resp) = query(pool, req).await.expect("querying data") {
        match http
            .post(req.destination.as_ref().unwrap())
            .timeout(time::Duration::from_secs(10))
            .json(&resp)
            .send()
            .await
        {
            Ok(http_resp) if http_resp.status().is_success() => {
                req.block_height = Some(resp.block_height + 1);
                tracing::Span::current()
                    .record("status", http_resp.status().to_string())
                    .record("block", resp.block_height);
            }
            Ok(http_resp) => {
                match http_resp
                    .bytes()
                    .await
                    .map(|b| String::from_utf8_lossy(&b[..100.min(b.len())]).into_owned())
                {
                    Ok(body) => tracing::error!("{}", body),
                    Err(err) => tracing::error!("{}", err),
                }
            }
            Err(http_err) => {
                tracing::error!("{}", http_err.to_string())
            }
        }
    }
}

#[tracing::instrument(skip_all fields(block = req.block_height))]
async fn query(
    pool: &Pool,
    req: &api_sql::Request,
) -> Result<Option<api_sql::Response>, api::Error> {
    let mut pg = pool.get().await.expect("unable to get pg from pool");
    let resp = api_sql::query(&mut pg, &vec![req.clone()]).await?;
    if resp.result.first().map_or(false, |v| !v.is_empty()) {
        Ok(Some(resp))
    } else {
        Ok(None)
    }
}
