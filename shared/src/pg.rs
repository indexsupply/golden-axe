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
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    pub async fn new(schema: &str) -> Pool {
        let db_name = random_db_name();
        let pool = super::new_pool("postgres://postgres:@localhost/postgres", 2).unwrap();
        pool.get()
            .await
            .expect("getting local postgres")
            .execute(&format!("create database {}", db_name), &[])
            .await
            .expect("creating database");
        let db_url = format!("postgres://postgres:@localhost:5432/{}", db_name);
        drop(pool);

        let pool = super::new_pool(&db_url, 2).unwrap();
        pool.get()
            .await
            .expect("getting conn from pool")
            .batch_execute(schema)
            .await
            .expect("setting up schema");
        pool
    }

    fn random_db_name() -> String {
        let random_str: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(6)
            .map(char::from)
            .collect();
        format!("be_test_{}", random_str.to_lowercase())
    }
}
