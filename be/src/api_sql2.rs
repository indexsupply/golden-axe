use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{self, Duration},
};

use alloy::{
    hex,
    primitives::{Bytes, I256},
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
use deadpool_postgres::Pool;
use eyre::Context;
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::Receiver;
use tokio_postgres::types::Type;

use crate::{
    api::{self},
    gafe, query,
};

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Request {
    #[serde(alias = "api-key")]
    pub api_key: Option<api::Key>,
    pub chains: HashMap<api::Chain, Option<u64>>,
    pub signatures: Vec<String>,
    pub query: String,
}

#[derive(Serialize)]
pub struct Response {
    pub chains: HashMap<api::Chain, Option<u64>>,
    pub columns: HashMap<String, String>,
    pub rows: Vec<Vec<Value>>,
}

pub async fn handle_post(
    Extension(log): Extension<RequestLog>,
    api_key: api::Key,
    State(config): State<api::Config>,
    account_limit: Arc<gafe::AccountLimit>,
    api::Json(mut req): api::Json<Vec<Request>>,
) -> Result<Json<Vec<Response>>, api::Error> {
    let ttl = account_limit.timeout;
    // api-key will be coming from the URL
    req.iter_mut().for_each(|r| {
        r.api_key.get_or_insert(api_key.clone());
    });
    log.add(req.clone());
    Ok(Json(query(config.ro_pool, ttl, &req).await?))
}

pub async fn handle_get(
    Extension(log): Extension<RequestLog>,
    State(config): State<api::Config>,
    account_limit: Arc<gafe::AccountLimit>,
    Form(req): Form<Request>,
) -> Result<Json<Vec<Response>>, api::Error> {
    let ttl = account_limit.timeout;
    log.add(vec![req.clone()]);
    Ok(Json(query(config.ro_pool, ttl, &[req]).await?))
}

#[tracing::instrument(skip_all)]
pub async fn handle_sse(
    Extension(log): Extension<RequestLog>,
    State(config): State<api::Config>,
    origin_ip: api::OriginIp,
    account_limit: Arc<gafe::AccountLimit>,
    Form(mut req): Form<Request>,
) -> Result<axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>>, api::Error> {
    log.add(vec![req.clone()]);

    let active_connections = config.new_connection().await?;
    let plan_limit = account_limit.conn_limiter()?;
    let ip_limit = account_limit.conn_ip_limiter(&origin_ip.to_string())?;

    let rx_keys: Vec<_> = req.chains.keys().cloned().collect();
    let stream = async_stream::stream! {
        let _hold_onto_permits = (active_connections, plan_limit, ip_limit);
        loop {
            match query(
                config.ro_pool.clone(),
                account_limit.timeout,
                &[req.clone()],
            )
            .await
            {
                Ok(resp) if resp.len() == 1 => {
                    req.chains = resp[0].chains.clone();
                    yield Ok(SSEvent::default().json_data(resp).unwrap());
                }
                Err(err) => {
                    yield Ok(SSEvent::default().json_data(err).unwrap());
                    return;
                }
                _ => {
                    yield Ok(SSEvent::default()
                        .json_data("unable to find query result")
                        .unwrap());
                    return;
                }
            }
            let mut futs = rx_keys
                .iter()
                .map(|id| {
                    let mut rx: Receiver<_> = config.api_updates.wait(*id);
                    async move { rx.recv().await }
                })
                .collect::<FuturesUnordered<_>>();
            match futs.next().await {
                Some(Ok(_)) => continue,
                Some(Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped))) => {
                    tracing::error!(skipped, "stream lagged")
                },
                Some(Err(tokio::sync::broadcast::error::RecvError::Closed)) | None => {
                    tracing::error!("stream closed. closing sse connection");
                    yield Ok(SSEvent::default().data(String::from("We're closed. Please come again!")));
                    return
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
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
                *req.chains.keys().next().unwrap(),
                req.signatures.clone(),
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

async fn query(
    be_pool: Pool,
    timeout: Duration,
    requests: &[Request],
) -> Result<Vec<Response>, api::Error> {
    let mut pg = be_pool.get().await?;
    let pgtx = pg
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .wrap_err("starting sql api read tx")?;
    pgtx.execute(
        &format!("set local statement_timeout = {}", timeout.as_millis()),
        &[],
    )
    .await?;
    let mut result: Vec<Response> = Vec::new();
    for r in requests {
        let sql = query::sql(
            &r.chains,
            r.signatures.iter().map(|s| s.as_str()).collect(),
            &r.query,
        )?;
        let rows = pgtx.query(&sql, &[]).await?;
        result.push(Response {
            chains: HashMap::new(),
            columns: get_columns(&rows)?,
            rows: get_rows(&rows)?,
        });
    }
    Ok(result)
}

fn get_columns(rows: &[tokio_postgres::Row]) -> Result<HashMap<String, String>, api::Error> {
    rows.first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|col| (col.name().to_string(), col.type_().to_string()))
                .collect()
        })
        .ok_or_else(|| api::Error::User("foo".to_string()))
}

fn get_rows(rows: &Vec<tokio_postgres::Row>) -> Result<Vec<Vec<Value>>, api::Error> {
    let mut result: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        let mut json_row: Vec<Value> = Vec::new();
        for (idx, column) in row.columns().iter().enumerate() {
            let value = match *column.type_() {
                Type::BOOL => match row.get::<usize, Option<bool>>(idx) {
                    Some(b) => Value::Bool(b),
                    None => Value::Bool(false),
                },
                Type::NUMERIC => match row.get::<usize, Option<I256>>(idx) {
                    Some(n) => Value::String(n.to_string()),
                    None => Value::Null,
                },
                Type::INT2 => match row.get::<usize, Option<i16>>(idx) {
                    Some(n) => Value::Number(n.into()),
                    None => Value::Null,
                },
                Type::INT4 => match row.get::<usize, Option<i32>>(idx) {
                    Some(n) => Value::Number(n.into()),
                    None => Value::Null,
                },
                Type::INT8 => match row.get::<usize, Option<i64>>(idx) {
                    Some(n) => Value::Number(n.into()),
                    None => Value::Null,
                },
                Type::BYTEA => match row.get::<usize, Option<&[u8]>>(idx) {
                    Some(b) => Value::String(hex::encode_prefixed(b)),
                    None => Value::Null,
                },
                Type::TEXT => match row.get::<usize, Option<String>>(idx) {
                    Some(s) => Value::String(s),
                    None => Value::Null,
                },
                Type::BYTEA_ARRAY => {
                    // for topics otherwise arrays are returned as jsonb via pg_golden_axe
                    let arrays: Vec<Vec<u8>> = row.get::<usize, Vec<Vec<u8>>>(idx);
                    serde_json::json!(arrays
                        .iter()
                        .map(|array| Bytes::copy_from_slice(array))
                        .collect_vec())
                }
                Type::JSON | Type::JSONB => row.get::<usize, serde_json::Value>(idx),
                _ => Value::Null,
            };
            json_row.push(value);
        }
        result.push(json_row)
    }
    Ok(result)
}
