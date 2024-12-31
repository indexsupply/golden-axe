use core::time;
use std::sync::Arc;

use alloy::primitives::U64;
use deadpool_postgres::Pool;
use eyre::Result;

use crate::{
    api,
    api_sql::{self, Request},
};

struct Webhook {
    pub request: api_sql::Request,
    pub id: U64,
    pub error: Option<String>,
}

async fn load_jobs(pool: &Pool) -> Result<Vec<Webhook>> {
    Ok(pool
        .get()
        .await?
        .query(
            "
            select
                w.*,
                wa.error as error,
                wa.block_height as block_height
            from webhooks w
            left join lateral (
                select error, block_height
                from webhook_attempts
                where webhook_id = w.id
                order by created_at desc
                limit 1
            ) wa on true
            ",
            &[],
        )
        .await?
        .iter()
        .map(|row| Webhook {
            id: row.get("id"),
            error: row.get("error"),
            request: Request {
                destination: row.get("destination"),
                block_height: row.get("block_height"),
                api_key: Some(api::Key(row.get::<&str, String>("api_key"))),
                chain: Some(api::Chain(row.get::<&str, U64>("chain").to())),
                event_signatures: row.get("event_signatures"),
                query: row.get("query"),
            },
        })
        .collect())
}

pub async fn run(gafe_pool: Pool, pool: Pool, broadcaster: Arc<api::Broadcaster>) -> Result<()> {
    let http_client = reqwest::Client::builder().build().unwrap();
    let webhooks = load_jobs(&gafe_pool).await.expect("loading jobs");
    for wh in webhooks {
        tokio::spawn(handle(
            gafe_pool.clone(),
            pool.clone(),
            http_client.clone(),
            broadcaster.clone(),
            wh,
        ));
    }
    Ok(())
}

#[tracing::instrument(skip_all fields(id, dest))]
async fn handle(
    gafe: Pool,
    pool: Pool,
    http: reqwest::Client,
    broadcaster: Arc<api::Broadcaster>,
    mut wh: Webhook,
) {
    tracing::Span::current()
        .record("id", wh.id.to::<u64>())
        .record("dest", wh.request.destination.as_ref().unwrap());
    let mut rx = broadcaster.wait(wh.request.chain.unwrap());
    loop {
        match rx.recv().await {
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!("closed");
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                tracing::info!("lagged");
            }
            Ok(_) => deliver(&http, &gafe, &pool, &mut wh).await,
        }
    }
}

#[tracing::instrument(skip_all fields(status, block))]
async fn deliver(http: &reqwest::Client, gafe: &Pool, pool: &Pool, wh: &mut Webhook) {
    if let Some(resp) = query(pool, &wh.request).await.expect("querying data") {
        match http
            .post(wh.request.destination.as_ref().unwrap())
            .timeout(time::Duration::from_secs(10))
            .json(&resp)
            .send()
            .await
        {
            Ok(http_resp) if http_resp.status().is_success() => {
                wh.request.block_height = Some(resp.block_height + U64::from(1));
                tracing::Span::current()
                    .record("status", http_resp.status().to_string())
                    .record("block", resp.block_height.to::<u64>());
                success(gafe, wh).await;
            }
            Ok(http_resp) => {
                match http_resp
                    .bytes()
                    .await
                    .map(|b| String::from_utf8_lossy(&b[..100.min(b.len())]).into_owned())
                {
                    Ok(body) => failure(gafe, wh, &body).await,
                    Err(err) => failure(gafe, wh, &err.to_string()).await,
                }
            }
            Err(http_err) => failure(gafe, wh, &http_err.to_string()).await,
        }
    }
}

#[tracing::instrument(skip_all fields(block = req.block_height.map(|t| t.to::<u64>())))]
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

async fn success(gafe: &Pool, wh: &Webhook) {
    gafe.get()
        .await
        .expect("getting pg from pool")
        .execute(
            "
            insert into webhook_attempts (webhook_id, block_height)
            values ($1, $2)
            ",
            &[&wh.id, &U64::from(wh.request.block_height.unwrap())],
        )
        .await
        .inspect_err(|e| tracing::error!("inserting attempt: {}", e))
        .ok();
}

async fn failure(gafe: &Pool, wh: &Webhook, error: &str) {
    gafe.get()
        .await
        .expect("getting pg from pool")
        .execute(
            "
            insert into webhook_attempts (webhook_id, block_height, error)
            values ($1, $2, $3)
            ",
            &[&wh.id, &wh.request.block_height.unwrap(), &error],
        )
        .await
        .inspect_err(|e| tracing::error!("inserting failed attempt: {}", e))
        .ok();
}
