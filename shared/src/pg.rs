use crate::Error;

use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{Context, Result};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::str::FromStr;

pub fn new_pool(url: &str, size: usize) -> Result<Pool> {
    let pg_config = tokio_postgres::Config::from_str(url)?;
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let pg_mgr = Manager::from_config(
        pg_config,
        connector,
        ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );
    Pool::builder(pg_mgr)
        .max_size(size)
        .build()
        .wrap_err("building pool")
}

pub fn unique_violations(err: tokio_postgres::Error, map: &[(&str, &str)]) -> Error {
    err.as_db_error()
        .filter(|e| e.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
        .and_then(|e| {
            map.iter()
                .find(|(c, _)| e.constraint().unwrap_or_default() == *c)
                .map(|(_, msg)| msg.to_string())
        })
        .map(Error::User)
        .unwrap_or_else(|| err.into())
}

#[cfg(feature = "test")]
pub mod test {
    use deadpool_postgres::Pool;
    use postgresql_embedded::{PostgreSQL, Settings, Version};
    use tokio_postgres::NoTls;

    pub async fn new(schema: &str) -> (PostgreSQL, Pool) {
        let pg_settings = Settings {
            version: Version::new(17, Some(2), Some(0)),
            ..Default::default()
        };
        let mut db = PostgreSQL::new(pg_settings);
        db.setup().await.expect("setting up pg");
        db.start().await.expect("starting pg");
        db.create_database("ga-test")
            .await
            .expect("creating test db");
        let mut pool_config = deadpool_postgres::Config::new();
        pool_config.url = Some(db.settings().url("ga-test"));
        let pool = pool_config
            .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
            .expect("creating pool");
        let pg = pool
            .get()
            .await
            .expect("unable to get test client from test pool");
        pg.batch_execute(schema).await.expect("resetting schema");
        (db, pool)
    }
}
