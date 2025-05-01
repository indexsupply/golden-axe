use std::sync::{Arc, Mutex};

use alloy::primitives::U64;
use axum::extract::State;

use crate::api;

#[derive(Clone)]
pub struct Row {
    pub api_key: String,
    pub ip: String,
    pub chain: u64,
    pub signatures: Vec<String>,
    pub user_query: String,
    pub latency: u16,
    pub status: u32,
    pub qty: u16,
}

impl Row {
    pub fn new(api_key: &str, chain: u64, signatures: Vec<String>, user_query: &str) -> Row {
        Row {
            api_key: api_key.to_string(),
            ip: String::new(),
            chain,
            signatures: signatures.clone(),
            user_query: user_query.to_string(),
            latency: 0,
            status: 0,
            qty: 1,
        }
    }
}

pub struct Rows {
    rows: Vec<Row>,
    start: std::time::SystemTime,
}

impl Default for Rows {
    fn default() -> Rows {
        Rows {
            rows: vec![],
            start: std::time::SystemTime::now(),
        }
    }
}

#[derive(Clone, Default)]
pub struct RequestLog(Arc<Mutex<Rows>>);

impl RequestLog {
    pub fn guard(&self, pool: deadpool_postgres::Pool, ip: String) -> Guard {
        Guard {
            ip,
            pool,
            log: self.clone(),
            status: 200,
        }
    }
    pub fn add(&self, rows: Vec<Row>) {
        self.0.lock().unwrap().rows = rows;
    }

    pub fn add_one(&self, row: Row) {
        self.0.lock().unwrap().rows.push(row)
    }

    pub fn incr(&self) {
        if let Some(row) = self.0.lock().unwrap().rows.iter_mut().next() {
            row.qty += 1
        }
    }

    async fn insert(self, pool: deadpool_postgres::Pool, status: u16, ip: String) {
        // only if no one else has the log
        if let Ok(log) = Arc::try_unwrap(self.0).map(Mutex::into_inner) {
            let log = log.unwrap();
            let latency = std::time::SystemTime::now()
                .duration_since(log.start)
                .unwrap()
                .as_millis() as u64;
            for mut row in log.rows {
                row.latency = latency as u16;
                row.status = status as u32;
                row.ip = ip.clone();
                insert(pool.clone(), row).await;
            }
        }
    }
}

pub async fn log_request(
    State(config): State<api::Config>,
    ip: api::OriginIp,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, api::Error> {
    let log: RequestLog = RequestLog(Arc::new(Mutex::new(Rows {
        rows: Vec::new(),
        start: std::time::SystemTime::now(),
    })));
    request.extensions_mut().insert(log.clone());
    let resp = next.run(request).await;
    let status = resp.status().as_u16();
    tokio::spawn(async move {
        log.insert(config.fe_pool, status, ip.to_string()).await;
    });
    Ok(resp)
}

#[tracing::instrument(level = "debug" skip_all)]
pub async fn insert(pool: deadpool_postgres::Pool, row: Row) {
    let timeout_res = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let res = pool
            .get()
            .await
            .expect("unable to get pg from pool")
            .query(
                "insert into user_queries (
                        api_key,
                        chain,
                        events,
                        user_query,
                        latency,
                        status,
                        ip,
                        qty
                    ) values ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &row.api_key,
                    &U64::from(row.chain),
                    &row.signatures,
                    &row.user_query,
                    &(row.latency as i32),
                    &(row.status as i16),
                    &row.ip,
                    &(row.qty as i16),
                ],
            )
            .await;
        if let Err(e) = res {
            tracing::error!("saving user query: {}", e);
        }
    })
    .await;
    if let Err(e) = timeout_res {
        tracing::error!("saving user query timeout: {}", e);
    }
}

pub struct Guard {
    log: RequestLog,
    pool: deadpool_postgres::Pool,
    ip: String,
    status: u16,
}

impl Guard {
    pub fn error(&mut self, err: &api::Error) {
        if let api::Error::User(_) = err {
            self.status = 400;
        } else {
            self.status = 500;
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let log = self.log.clone();
        let pool = self.pool.clone();
        let ip = self.ip.clone();
        let status = self.status;
        tokio::spawn(async move {
            log.insert(pool, status, ip).await;
        });
    }
}
