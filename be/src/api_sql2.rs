use std::{convert::Infallible, sync::Arc, time::Duration};

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
use deadpool_postgres::Pool;
use eyre::{eyre, Context};
use futures::Stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;

use crate::{api, cursor, gafe, query, s256, user_query};

impl From<&Request> for user_query::Row {
    fn from(req: &Request) -> user_query::Row {
        let api_key = req
            .api_key
            .as_ref()
            .map(|k| k.to_string())
            .unwrap_or_default();
        user_query::Row::new(
            &api_key,
            req.cursor.chain(),
            req.signatures.clone(),
            &req.query,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Request {
    #[serde(alias = "api-key")]
    pub api_key: Option<api::Key>,
    #[serde(default)]
    pub cursor: cursor::Cursor,
    #[serde(default)]
    pub signatures: Vec<String>,
    pub query: String,
}

#[derive(Serialize)]
pub struct Column {
    pub name: String,
    pub pgtype: String,
}

#[derive(Serialize)]
pub struct Response {
    pub cursor: cursor::Cursor,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
}

pub async fn handle_post(
    Extension(log): Extension<user_query::RequestLog>,
    api_key: api::Key,
    State(config): State<api::Config>,
    al: Arc<gafe::AccountLimit>,
    api::Json(mut req): api::Json<Vec<Request>>,
) -> Result<Json<Vec<Response>>, api::Error> {
    // api-key will be coming from the URL
    req.iter_mut().for_each(|r| {
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
) -> Result<Json<Vec<Response>>, api::Error> {
    log.add_one((&req).into());
    Ok(Json(query(config.ro_pool, al.timeout, &[req]).await?))
}

#[tracing::instrument(skip_all, fields(cursor))]
pub async fn handle_sse(
    Extension(log): Extension<user_query::RequestLog>,
    State(config): State<api::Config>,
    ip: api::OriginIp,
    al: Arc<gafe::AccountLimit>,
    Form(mut req): Form<Request>,
) -> Result<axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>>, api::Error> {
    tracing::Span::current().record("cursor", req.cursor.to_string());
    let active_connections = config.new_connection().await?;
    let plan_limit = al.conn_limiter()?;
    let ip_limit = al.conn_ip_limiter(&ip.to_string())?;

    log.add_one((&req).into());
    let stream = async_stream::stream! {
        let _hold_onto_permits = (active_connections, plan_limit, ip_limit);
        let mut log_guard = log.guard(config.fe_pool.clone(), ip.to_string());
        loop {
            match query(
                config.ro_pool.clone(),
                al.timeout,
                &[req.clone()],
            )
            .await
            {
                Ok(resp) if resp.len() == 1 => {
                    log.incr();
                    req.cursor = resp[0].cursor.clone();
                    yield Ok(SSEvent::default().json_data(&resp).unwrap());
                }
                Err(err) => {
                    log_guard.error(&err);
                    yield Ok(SSEvent::default().json_data(err).unwrap());
                    return;
                },
                _ => {
                    let err = api::Error::Server(eyre!("expected only one result").into());
                    log_guard.error(&err);
                    yield Ok(SSEvent::default().json_data(err).unwrap());
                    return;
                }

            }
            let waiting = config
                .broadcaster
                .wait(&req.cursor.chains())
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
        let mut cursor = r.cursor.clone();
        let q = query::sql(
            &mut cursor,
            r.signatures.iter().map(|s| s.as_str()).collect(),
            &r.query,
        )?;
        let rows = pgtx.query(&q, &[]).await?;
        update_cursor(&pgtx, &mut cursor).await?;
        result.push(Response {
            cursor,
            columns: get_columns(&rows),
            rows: get_rows(&rows),
        });
    }
    Ok(result)
}

async fn update_cursor(
    pgtx: &tokio_postgres::Transaction<'_>,
    cursor: &mut cursor::Cursor,
) -> Result<(), api::Error> {
    for c in cursor.chains() {
        let row = pgtx
            .query_one(
                "select coalesce(max(num), 0) from blocks where chain = $1",
                &[&U64::from(c)],
            )
            .await?;
        let latest: u64 = row.get::<usize, U64>(0).to();
        cursor.set_block_height(c, latest + 1);
    }
    Ok(())
}

fn get_columns(rows: &[tokio_postgres::Row]) -> Vec<Column> {
    rows.first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|col| Column {
                    name: col.name().to_string(),
                    pgtype: col.type_().to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn get_rows(rows: &[tokio_postgres::Row]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(idx, col)| value_from_column(row, idx, col))
                .collect()
        })
        .collect()
}

fn value_from_column(
    row: &tokio_postgres::Row,
    idx: usize,
    column: &tokio_postgres::Column,
) -> Value {
    match *column.type_() {
        Type::BOOL => row
            .get::<usize, Option<bool>>(idx)
            .map(Value::Bool)
            .unwrap_or(Value::Bool(false)),
        Type::NUMERIC => row
            .get::<usize, Option<s256::Int>>(idx)
            .map(|n| Value::String(n.to_string()))
            .unwrap_or(Value::Null),
        Type::INT2 => row
            .get::<usize, Option<i16>>(idx)
            .map(|n| Value::Number(n.into()))
            .unwrap_or(Value::Null),
        Type::INT4 => row
            .get::<usize, Option<i32>>(idx)
            .map(|n| Value::Number(n.into()))
            .unwrap_or(Value::Null),
        Type::INT8 => row
            .get::<usize, Option<i64>>(idx)
            .map(|n| Value::Number(n.into()))
            .unwrap_or(Value::Null),
        Type::BYTEA => row
            .get::<usize, Option<&[u8]>>(idx)
            .map(|b| Value::String(hex::encode_prefixed(b)))
            .unwrap_or(Value::Null),
        Type::TEXT => row
            .get::<usize, Option<String>>(idx)
            .map(Value::String)
            .unwrap_or(Value::Null),
        Type::DATE => row
            .get::<usize, Option<time::Date>>(idx)
            .map(|t| Value::String(t.to_string()))
            .unwrap_or(Value::Null),
        Type::TIMESTAMPTZ => row
            .get::<usize, Option<time::OffsetDateTime>>(idx)
            .map(|t| Value::String(t.to_string()))
            .unwrap_or(Value::Null),
        Type::BYTEA_ARRAY => {
            let arrays: Vec<Vec<u8>> = row.get(idx);
            serde_json::json!(arrays
                .iter()
                .map(|a| Bytes::copy_from_slice(a.as_slice()))
                .collect_vec())
        }
        Type::JSON | Type::JSONB => row.get(idx),
        _ => Value::Null,
    }
}
