use std::{convert::Infallible, sync::Arc, time::Duration};

use alloy::{
    hex,
    primitives::{Bytes, I256, U64},
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
use futures::Stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;

use crate::{
    api::{self},
    cursor, gafe, query, user_query,
};

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Request {
    #[serde(alias = "api-key")]
    pub api_key: Option<api::Key>,
    pub chain: Option<u64>,
    pub event_signatures: Vec<String>,
    pub query: String,
    pub block_height: Option<u64>,
}

impl From<&Request> for user_query::Row {
    fn from(req: &Request) -> user_query::Row {
        let api_key = req
            .api_key
            .as_ref()
            .map(|k| k.to_string())
            .unwrap_or_default();
        user_query::Row::new(
            &api_key,
            req.chain.unwrap_or_default(),
            req.event_signatures.clone(),
            &req.query,
        )
    }
}

type Row = Vec<Value>;
type Rows = Vec<Row>;

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    pub block_height: u64,
    pub result: Vec<Rows>,
}

pub async fn handle_post(
    Extension(log): Extension<user_query::RequestLog>,
    api_key: api::Key,
    chain: api::Chain,
    State(config): State<api::Config>,
    al: Arc<gafe::AccountLimit>,
    api::Json(mut req): api::Json<Vec<Request>>,
) -> Result<Json<Response>, api::Error> {
    // It's possible to specify chain/api_key in either the header or the query params for POST
    req.iter_mut().for_each(|r| {
        r.chain.get_or_insert(chain.0);
        r.api_key.get_or_insert(api_key.clone());
    });
    log.add(req.iter().map(|r| r.into()).collect());
    Ok(Json(query(config.ro_pool, al.timeout, &req).await?))
}

pub async fn handle_get(
    Extension(log): Extension<user_query::RequestLog>,
    State(config): State<api::Config>,
    al: Arc<gafe::AccountLimit>,
    Form(req): Form<Request>,
) -> Result<Json<Response>, api::Error> {
    log.add_one((&req).into());
    Ok(Json(query(config.ro_pool, al.timeout, &[req]).await?))
}

#[tracing::instrument(skip_all)]
pub async fn handle_sse(
    Extension(log): Extension<user_query::RequestLog>,
    State(config): State<api::Config>,
    ip: api::OriginIp,
    al: Arc<gafe::AccountLimit>,
    Form(mut req): Form<Request>,
) -> Result<axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>>, api::Error> {
    let active_connections = config.new_connection().await?;
    let plan_limit = al.conn_limiter()?;
    let ip_limit = al.conn_ip_limiter(&ip.to_string())?;

    log.add_one((&req).into());
    let stream = async_stream::stream! {
        let _hold_onto_permits = (active_connections, plan_limit, ip_limit);
        let mut log_guard = log.guard(config.fe_pool.clone(), ip.to_string());
        loop {
            match query(config.ro_pool.clone(), al.timeout, &[req.clone()]).await {
                Ok(resp) =>  {
                    log.incr();
                    req.block_height = Some(resp.block_height + 1);
                    yield Ok(SSEvent::default().json_data(resp).expect("sse serialize query"));
                },
                Err(err) => {
                    log_guard.error(&err);
                    yield Ok(SSEvent::default().json_data(err).expect("sse serialize error"));
                    return;
                }
            }
            let waiting = config
                .broadcaster
                .wait(&[req.chain.unwrap()])
                .await;
            if waiting.is_none() {
                yield Ok(SSEvent::default().json_data("closed").expect("sse serialize error"));
                return;
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn query(
    be_pool: Pool,
    timeout: Duration,
    requests: &[Request],
) -> Result<Response, api::Error> {
    let queries = requests
        .iter()
        .map(|r| {
            query::sql(
                &mut cursor::Cursor::new(r.chain.unwrap_or_default(), r.block_height),
                r.event_signatures.iter().map(|s| s.as_str()).collect(),
                &r.query,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
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
    let chain = requests
        .first()
        .expect("no queries in request")
        .chain
        .unwrap_or_default();
    let block_height = pgtx
        .query_one(
            "select coalesce(max(num), 0)::text from blocks where chain = $1",
            &[&U64::from(chain)],
        )
        .await?
        .get::<usize, U64>(0)
        .to::<u64>();
    let mut result: Vec<Rows> = Vec::new();
    for q in queries {
        result.push(handle_rows(pgtx.query(&q, &[]).await?)?);
    }
    Ok(Response {
        block_height,
        result,
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
                Type::DATE => row
                    .get::<usize, Option<time::Date>>(idx)
                    .map(|t| Value::String(t.to_string()))
                    .unwrap_or(Value::Null),
                Type::TIMESTAMPTZ => row
                    .get::<usize, Option<time::OffsetDateTime>>(idx)
                    .map(|t| Value::String(t.to_string()))
                    .unwrap_or(Value::Null),
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
