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
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use url::Url;

    const PREFIX: &str = "golden_axe_test_";
    const MAX_AGE: Duration = Duration::from_secs(300);

    pub async fn new(schema: &str) -> Pool {
        let admin_db_url = "postgres://postgres:@localhost:5432/golden_axe_test";
        let admin = super::new_pool(admin_db_url, 1).unwrap();
        cleanup_old_dbs(&admin).await;

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rand: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let name = format!("{PREFIX}{ts}_{rand}").to_lowercase();

        admin
            .get()
            .await
            .expect("admin conn")
            .execute(&format!("CREATE DATABASE {name}"), &[])
            .await
            .expect("create db");

        let mut u = Url::parse(admin_db_url).unwrap();
        u.set_path(&format!("/{}", name));
        let db_url = u.to_string();

        drop(admin);

        let pool = super::new_pool(&db_url, 2).unwrap();
        pool.get()
            .await
            .expect("db conn")
            .batch_execute(schema)
            .await
            .expect("schema");
        pool
    }

    async fn cleanup_old_dbs(admin: &Pool) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let conn = admin.get().await.unwrap();
        let rows = conn
            .query(
                "SELECT datname FROM pg_database WHERE datname LIKE $1 AND datname <> current_database()",
                &[&format!("{PREFIX}%")],
            )
            .await
            .unwrap();

        for row in rows {
            let name: String = row.get(0);
            if let Some(ts) = parse_ts(&name) {
                if now.saturating_sub(ts) > MAX_AGE.as_secs() {
                    let _ = conn
                        .execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"), &[])
                        .await;
                }
            }
        }
    }

    fn parse_ts(name: &str) -> Option<u64> {
        let rest = name.strip_prefix(PREFIX)?;
        let ts = rest.split('_').next()?;
        ts.parse().ok()
    }
}
