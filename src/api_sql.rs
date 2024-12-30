use std::convert::Infallible;

use alloy::{
    hex,
    primitives::{Bytes, U64},
};
use axum::{
    extract::State,
    response::{
        sse::{Event as SSEvent, KeepAlive},
        Sse,
    },
    Json,
};
use axum_extra::extract::Form;
use deadpool_postgres::Pool;
use eyre::{Context, Result};
use futures::Stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{types::Type, Client};

use crate::{
    api::{self, ChainOptionExt},
    log_query, s256, sql_generate,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Request {
    pub destination_url: Option<String>,
    #[serde(alias = "api-key")]
    pub api_key: Option<api::Key>,
    pub chain: Option<api::Chain>,
    pub event_signatures: Vec<String>,
    pub query: String,
    pub block_height: Option<u64>,
}

type Row = Vec<Value>;
type Rows = Vec<Row>;

#[derive(Deserialize, Serialize)]
pub struct Response {
    pub block_height: u64,
    pub result: Vec<Rows>,
}

pub async fn handle_post(
    api_key: api::Key,
    chain: api::Chain,
    State(config): State<api::Config>,
    api::Json(mut req): api::Json<Vec<Request>>,
) -> Result<Json<Response>, api::Error> {
    // It's possible to specify chain/api_key in either the header or the query params for POST
    req.iter_mut().for_each(|r| {
        r.chain.get_or_insert(chain);
        r.api_key.get_or_insert(api_key.clone());
    });
    let mut pg = config.pool.get().await.wrap_err("getting conn from pool")?;
    log_query!(config.gafe, batch: req, { Ok(Json(query(&mut pg, &req).await?)) })
}

pub async fn handle_get(
    State(config): State<api::Config>,
    Form(req): Form<Request>,
) -> Result<Json<Response>, api::Error> {
    let mut pg = config.pool.get().await.wrap_err("getting conn from pool")?;
    log_query!(config.gafe, single: req, {
        Ok(Json(query(&mut pg, &vec![req]).await?))
    })
}

pub async fn handle_sse(
    State(config): State<api::Config>,
    Form(mut req): Form<Request>,
) -> axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>> {
    log_query!(config.gafe, &req);
    let mut rx = config.broadcaster.wait(req.chain.expect("missing chain"));
    let stream = async_stream::stream! {
        loop {
            {
                let mut pg = config.pool.get().await.expect("unable to get pg from pool");
                let resp = query(&mut pg, &vec![req.clone()]).await.expect("unable to make request");
                req.block_height = Some(resp.block_height + 1);
                yield Ok(SSEvent::default().json_data(resp).expect("unable to serialize json"));
            }
            match rx.recv().await {
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::error!("stream closed. closing sse connection");
                    return
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::error!(skipped, "stream lagged")
                }
                Ok(_) => {},
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn webhooks(pool: Pool, broadcaster: api::Broadcaster) {
    let requests: Vec<Request> = vec![];
    for request in requests {
        let pool = pool.clone();
        let mut req = request.clone();
        let mut rx = broadcaster.wait(req.chain.unwrap());
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Ok(_) => match deliver(pool.clone(), &mut req).await {
                        Err(e) => tracing::error!("failed webhook {:?}", e),
                        Ok(()) => tracing::info!("successful webhook"),
                    },
                }
            }
        });
    }
}

async fn deliver(pool: Pool, req: &mut Request) -> Result<(), api::Error> {
    let dest_url = req
        .destination_url
        .as_ref()
        .ok_or(api::Error::User(String::from("missing destination url")))?;
    let resp = response(pool.clone(), req).await?;
    post(dest_url, &resp).await?;
    req.block_height = Some(resp.block_height + 1);
    save_attempt(pool, req).await?;
    Ok(())
}

async fn response(pool: Pool, req: &Request) -> Result<Response, api::Error> {
    let mut pg = pool.get().await.expect("unable to get pg from pool");
    query(&mut pg, &vec![req.clone()]).await
}

async fn post(url: &str, resp: &Response) -> Result<(), api::Error> {
    todo!()
}

async fn save_attempt(pool: Pool, req: &mut Request) -> Result<(), api::Error> {
    todo!()
}

async fn query(pg: &mut Client, requests: &Vec<Request>) -> Result<Response, api::Error> {
    let pgtx = pg
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .wrap_err("starting sql api read tx")?;
    let mut result: Vec<Rows> = Vec::new();
    for r in requests {
        let query = sql_generate::query(
            r.chain.unwrap_chain()?,
            r.block_height,
            &r.query,
            r.event_signatures.iter().map(|s| s.as_str()).collect(),
        )?;
        result.push(handle_rows(pgtx.query(&query, &[]).await?)?);
    }
    Ok(Response {
        result,
        block_height: pgtx
            .query_one(
                "select coalesce(max(num), 0)::text from blocks where chain = $1",
                &[&requests
                    .first()
                    .expect("no queries in request")
                    .chain
                    .unwrap_chain()?],
            )
            .await?
            .get::<usize, U64>(0)
            .to::<u64>(),
    })
}

fn handle_rows(rows: Vec<tokio_postgres::Row>) -> Result<Rows, api::Error> {
    let mut result: Rows = Vec::new();
    if let Some(first) = rows.first() {
        result.push(
            first
                .columns()
                .iter()
                .map(|c| Value::String(c.name().to_string()))
                .collect(),
        );
    }
    for row in rows {
        let mut json_row: Vec<Value> = Vec::new();
        for (idx, column) in row.columns().iter().enumerate() {
            let value = match *column.type_() {
                Type::BOOL => {
                    let b: bool = row.get(idx);
                    Value::Bool(b)
                }
                Type::NUMERIC => {
                    let n: Option<s256::Int> = row.get(idx);
                    match n {
                        Some(n) => Value::String(n.to_string()),
                        None => Value::Null,
                    }
                }
                Type::INT2 => {
                    let n: i16 = row.get(idx);
                    Value::Number(n.into())
                }
                Type::INT4 => {
                    let n: i32 = row.get(idx);
                    Value::Number(n.into())
                }
                Type::INT8 => {
                    let n: i64 = row.get(idx);
                    Value::Number(n.into())
                }
                Type::BYTEA => {
                    let b: &[u8] = row.get(idx);
                    Value::String(hex::encode_prefixed(b))
                }
                Type::TEXT => {
                    let s: String = row.get(idx);
                    Value::String(s)
                }
                Type::NUMERIC_ARRAY => {
                    let nums: Vec<s256::Int> = row.get(idx);
                    serde_json::json!(nums.iter().map(|n| n.to_string()).collect::<Vec<String>>())
                }
                Type::BYTEA_ARRAY => {
                    let arrays: Vec<Vec<u8>> = row.get::<usize, Vec<Vec<u8>>>(idx);
                    serde_json::json!(arrays
                        .iter()
                        .map(|array| Bytes::copy_from_slice(array))
                        .collect_vec())
                }
                _ => Value::Null,
            };
            json_row.push(value);
        }
        result.push(json_row)
    }
    Ok(result)
}
