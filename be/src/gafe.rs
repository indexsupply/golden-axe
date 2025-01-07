use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use deadpool_postgres::Pool;
use governor::{Quota, RateLimiter};
use nonzero::nonzero;

use crate::api;

#[derive(Debug)]
pub struct AccountLimit {
    secret: String,
    pub origins: HashSet<String>,
    pub timeout: Duration,
    pub rate: Arc<governor::DefaultKeyedRateLimiter<String>>,
}

impl AccountLimit {
    pub fn free() -> Self {
        AccountLimit {
            secret: String::default(),
            origins: HashSet::new(),
            timeout: Duration::from_secs(10),
            rate: Arc::new(governor::DefaultKeyedRateLimiter::dashmap(
                Quota::per_minute(nonzero!(10u32)),
            )),
        }
    }
    // something is wrong with our system so don't impact users
    pub fn open() -> Self {
        AccountLimit {
            secret: String::default(),
            origins: HashSet::new(),
            timeout: Duration::from_secs(10),
            rate: Arc::new(governor::DefaultKeyedRateLimiter::dashmap(
                Quota::per_second(nonzero!(10u32)),
            )),
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
                "select secret, timeout, rate, origins from account_limits",
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
                    rate: Arc::new(RateLimiter::keyed(Quota::per_second(
                        NonZeroU32::new(row.get::<&str, i32>("rate") as u32).unwrap(),
                    ))),
                    origins: row
                        .get::<&str, Vec<String>>("origins")
                        .into_iter()
                        .map(|s| s.to_lowercase())
                        .map(|s| s.trim().to_string())
                        .collect(),
                })
                .map(|al| (al.secret.clone(), Arc::new(al)))
                .collect(),
        )
    }

    #[tracing::instrument(level = "debug" skip_all)]
    pub async fn log_query(
        &self,
        key: Option<api::Key>,
        chain: api::Chain,
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
                            status
                        ) values ($1, $2, $3, $4, $5, $6)",
                        &[
                            &key.map(|k| k.to_string()),
                            &chain,
                            &events,
                            &query,
                            &(latency as i32),
                            &(status as i16),
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
