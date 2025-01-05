use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{self},
};

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
    Extension, Json,
};
use axum_extra::extract::Form;
use eyre::{Context, Result};
use futures::Stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{types::Type, Client};

use crate::{
    api::{self, ChainOptionExt},
    gafe, s256, sql_generate,
};

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Request {
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
    Extension(log): Extension<RequestLog>,
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
    log.add(req.clone());
    let mut pg = config.pool.get().await.wrap_err("getting conn from pool")?;
    Ok(Json(query(&mut pg, &req).await?))
}

pub async fn handle_get(
    Extension(log): Extension<RequestLog>,
    State(config): State<api::Config>,
    Form(req): Form<Request>,
) -> Result<Json<Response>, api::Error> {
    log.add(vec![req.clone()]);
    let mut pg = config.pool.get().await.wrap_err("getting conn from pool")?;
    Ok(Json(query(&mut pg, &vec![req]).await?))
}

pub async fn handle_sse(
    Extension(log): Extension<RequestLog>,
    State(config): State<api::Config>,
    Form(mut req): Form<Request>,
) -> axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>> {
    log.add(vec![req.clone()]);
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

#[derive(Clone)]
pub struct RequestMeta {
    requests: Vec<Request>,
    start: time::SystemTime,
}

#[derive(Clone)]
pub struct RequestLog(Arc<Mutex<RequestMeta>>);

impl RequestLog {
    fn add(&self, requests: Vec<Request>) {
        self.0.lock().unwrap().requests = requests;
    }

    async fn done(&self, gafe: gafe::Connection, status: u16) {
        let log = self.0.lock().unwrap().clone();
        let latency = std::time::SystemTime::now()
            .duration_since(log.start)
            .unwrap()
            .as_millis() as u64;
        for req in &log.requests {
            gafe.log_query(
                req.api_key.clone(),
                req.chain.unwrap_or_default(),
                req.event_signatures.clone(),
                req.query.clone(),
                latency,
                status,
            )
            .await
        }
    }
}

pub async fn log_request(
    State(config): State<api::Config>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, api::Error> {
    let log: RequestLog = RequestLog(Arc::new(Mutex::new(RequestMeta {
        requests: Vec::new(),
        start: time::SystemTime::now(),
    })));
    request.extensions_mut().insert(log.clone());
    let resp = next.run(request).await;
    log.done(config.gafe, resp.status().as_u16()).await;
    Ok(resp)
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
