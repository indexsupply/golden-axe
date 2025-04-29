use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use alloy::primitives::U64;
use dashmap::DashMap;
use deadpool_postgres::Pool;
use governor::{Quota, RateLimiter};
use nonzero::nonzero;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    api::{self},
    cursor,
};

#[derive(Debug)]
pub struct AccountLimit {
    secret: String,
    pub origins: HashSet<String>,
    pub timeout: Duration,
    pub rate: i32,
    pub rate_limiter: Arc<governor::DefaultKeyedRateLimiter<String>>,
    pub connections: i32,
    pub conn_limiter: Arc<Semaphore>,
    pub ip_connections: Option<i32>,
    pub ip_conn_limiter: DashMap<String, Arc<Semaphore>>,
}

impl PartialEq for AccountLimit {
    fn eq(&self, other: &Self) -> bool {
        self.secret == other.secret
            && self.origins == other.origins
            && self.timeout == other.timeout
            && self.rate == other.rate
            && self.connections == other.connections
            && self.ip_connections == other.ip_connections
    }
}

impl AccountLimit {
    pub fn free() -> Self {
        AccountLimit {
            secret: String::default(),
            origins: HashSet::new(),
            timeout: Duration::from_secs(10),
            rate: 10,
            rate_limiter: Arc::new(governor::DefaultKeyedRateLimiter::dashmap(
                Quota::per_minute(nonzero!(10u32)),
            )),
            connections: 100,
            conn_limiter: Arc::new(Semaphore::new(100)),
            ip_connections: Some(1),
            ip_conn_limiter: DashMap::new(),
        }
    }
    // something is wrong with our system so don't impact users
    pub fn open() -> Self {
        AccountLimit {
            secret: String::default(),
            origins: HashSet::new(),
            timeout: Duration::from_secs(10),
            rate: 10,
            rate_limiter: Arc::new(governor::DefaultKeyedRateLimiter::dashmap(
                Quota::per_second(nonzero!(10u32)),
            )),
            connections: 1000,
            conn_limiter: Arc::new(Semaphore::new(1000)),
            ip_connections: Some(5),
            ip_conn_limiter: DashMap::new(),
        }
    }

    pub fn conn_limiter(&self) -> Result<OwnedSemaphorePermit, api::Error> {
        self.conn_limiter.clone().try_acquire_owned().map_err(|_| {
            api::Error::TooManyRequests(Some("too many connections from this account".into()))
        })
    }

    pub fn conn_ip_limiter(&self, ip: &str) -> Result<Option<OwnedSemaphorePermit>, api::Error> {
        match self.ip_connections {
            Some(i) => self
                .ip_conn_limiter
                .entry(ip.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(i as usize)))
                .clone()
                .try_acquire_owned()
                .map(Some)
                .map_err(|_| {
                    api::Error::TooManyRequests(Some("too many connections from this IP".into()))
                }),
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct Connection {
    fe_pool: Pool,
    enabled: Arc<AtomicBool>,
}

impl Connection {
    pub fn new(fe_pool: Pool) -> Connection {
        Connection {
            fe_pool,
            enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    fn disable(&self) {
        self.enabled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tracing::instrument(skip_all)]
    pub async fn load_account_limits(&self) -> Option<HashMap<String, Arc<AccountLimit>>> {
        let res = self
            .fe_pool
            .get()
            .await
            .map_err(|err| {
                self.disable();
                tracing::error!("loading account limits: {}", err)
            })
            .ok()?
            .query(
                "select secret, timeout, rate, connections, ip_connections, origins from account_limits",
                &[],
            )
            .await
            .map_err(|err| {
                self.disable();
                tracing::error!("loading account limits: {}", err)
            })
            .ok()?;
        Some(
            res.iter()
                .map(|row| AccountLimit {
                    secret: row.get("secret"),
                    timeout: Duration::from_secs(row.get::<&str, i32>("timeout") as u64),
                    origins: row
                        .get::<&str, Vec<String>>("origins")
                        .into_iter()
                        .map(|s| s.to_lowercase())
                        .map(|s| s.trim().to_string())
                        .collect(),
                    rate: row.get("rate"),
                    rate_limiter: Arc::new(RateLimiter::keyed(Quota::per_second(
                        NonZeroU32::new(row.get::<&str, i32>("rate") as u32).unwrap(),
                    ))),
                    connections: row.get("connections"),
                    conn_limiter: Arc::new(Semaphore::new(
                        row.get::<&str, i32>("connections") as usize
                    )),
                    ip_connections: row.get("ip_connections"),
                    ip_conn_limiter: DashMap::new(),
                })
                .map(|al| (al.secret.clone(), Arc::new(al)))
                .collect(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug" skip_all)]
    pub async fn log_query(
        &self,
        key: Option<api::Key>,
        ip: api::OriginIp,
        cursor: cursor::Cursor,
        events: Vec<String>,
        query: String,
        latency: u64,
        status: u16,
    ) {
        let fe_pool = self.fe_pool.clone();
        tokio::spawn(async move {
            let timeout_res = tokio::time::timeout(Duration::from_secs(1), async {
                let res = fe_pool
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
                            ip
                        ) values ($1, $2, $3, $4, $5, $6, $7)",
                        &[
                            &key.map(|k| k.to_string()),
                            &U64::from(cursor.chain()),
                            &events,
                            &query,
                            &(latency as i32),
                            &(status as i16),
                            &ip.to_string(),
                        ],
                    )
                    .await;
                if res.is_err() {
                    tracing::error!("logging user query: {:?}", res);
                }
            })
            .await;
            if timeout_res.is_err() {
                tracing::error!("logging user query timed out");
            }
        });
    }
}
