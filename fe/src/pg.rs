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
