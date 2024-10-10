use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use eyre::{Context, Result};
use governor::{Quota, RateLimiter};
use nonzero::nonzero;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio::sync::Mutex;
use tokio_postgres::Client;

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
    pg: Arc<Mutex<Option<Client>>>,
    live: Arc<AtomicBool>,
}

async fn pg(url: &str) -> Result<Client> {
    let mut builder = SslConnector::builder(SslMethod::tls()).wrap_err("building tls")?;
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (pg, conn) = tokio_postgres::connect(url, connector)
        .await
        .expect("starting connection");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            panic!("gafe database writer error: {}", e)
        }
    });
    Ok(pg)
}

impl Connection {
    pub async fn new(pg_url: Option<String>) -> Connection {
        match pg_url {
            None => Connection {
                pg: Arc::new(Mutex::new(None)),
                live: Arc::new(AtomicBool::new(false)),
            },
            Some(url) => match pg(&url).await {
                Ok(pg) => Connection {
                    pg: Arc::new(Mutex::new(Some(pg))),
                    live: Arc::new(AtomicBool::new(true)),
                },
                Err(e) => {
                    tracing::error!("unable to connect to gafe: {:?}", e);
                    Connection {
                        pg: Arc::new(Mutex::new(None)),
                        live: Arc::new(AtomicBool::new(false)),
                    }
                }
            },
        }
    }

    pub async fn live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }

    #[tracing::instrument(skip_all)]
    pub async fn load_account_limits(
        &self,
        chain_id: i64,
    ) -> Option<HashMap<String, Arc<AccountLimit>>> {
        let pg_opt = self.pg.lock().await;
        if pg_opt.is_none() {
            tracing::info!("gafe pg not configured");
            return None;
        }
        let pg = pg_opt.as_ref().unwrap();
        let res = pg
            .query(
                "
                select encode(secret, 'hex') as secret, timeout, rate, origins
                from account_limits where $1 = any(chains)
                ",
                &[&chain_id],
            )
            .await;
        if let Err(e) = res {
            tracing::error!("loading account limits: {:?}", e);
            return None;
        }
        Some(
            res.unwrap()
                .iter()
                .map(|row| AccountLimit {
                    secret: row.get("secret"),
                    timeout: Duration::from_secs(row.get::<&str, i32>("timeout") as u64),
                    rate: Arc::new(RateLimiter::keyed(Quota::per_second(
                        NonZeroU32::new(row.get::<&str, i32>("rate") as u32).unwrap(),
                    ))),
                    origins: row
                        .get::<&str, Vec<String>>("origins")
                        .into_iter()
                        .collect(),
                })
                .map(|al| (al.secret.clone(), Arc::new(al)))
                .collect(),
        )
    }
}
