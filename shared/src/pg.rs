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
    use std::{path::PathBuf, time::Duration};

    use deadpool_postgres::Pool;
    use eyre::{eyre, Result};
    use flate2::read::GzDecoder;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use tar::Archive;
    use tokio::{
        fs::{self},
        io::{AsyncBufReadExt, BufReader},
        process::Command,
        sync::Mutex,
    };

    static RUNNING_MUTEX: Mutex<bool> = Mutex::const_new(false);

    pub async fn new(schema: &str) -> Pool {
        let mut running = RUNNING_MUTEX.lock().await;
        if *running {
            tracing::info!("postgresql already running");
        } else {
            tracing::info!("starting postgresql");
            for i in 0..5 {
                match start_postgresql().await {
                    Ok(_) => {
                        *running = true;
                        break;
                    }
                    Err(e) if i == 4 => panic!("starting pg: {e}"),
                    Err(e) => tracing::error!("error starting pg {}", e),
                }
            }
        }
        let db_name = random_db_name();
        let pool = super::new_pool("postgres://localhost:7999/postgres", 1).unwrap();
        pool.get()
            .await
            .expect("getting local postgres")
            .execute(&format!("create database {db_name}"), &[])
            .await
            .expect("creating database");
        let db_url = format!("postgres://localhost:7999/{db_name}");
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

    const PG_VERSION: &str = "18.1.0";

    fn pg_install_dir() -> String {
        format!("postgresql-{}-{}", PG_VERSION, target_triple::TARGET)
    }

    fn pg_path() -> PathBuf {
        PathBuf::from("/tmp/golden-axe-pg-test")
    }

    pub async fn download_postgresql() -> Result<()> {
        let install_dir = pg_path().join("install");
        let url = format!(
            "https://github.com/theseus-rs/postgresql-binaries/releases/download/{}/{}.tar.gz",
            PG_VERSION,
            pg_install_dir(),
        );
        if fs::read_dir(&install_dir).await.iter().next().is_some() {
            tracing::info!("postgres already downloaded");
            return Ok(());
        }
        tracing::info!("downloading postgres");
        fs::create_dir_all(&install_dir).await?;
        let response = reqwest::get(url).await?;
        if !response.status().is_success() {
            return Err(eyre!("unable to download pg"));
        }
        let content = response.bytes().await?;
        let tar = GzDecoder::new(content.as_ref());
        let mut archive = Archive::new(tar);
        archive.unpack(install_dir).unwrap();
        Ok(())
    }

    pub async fn check_pg() -> bool {
        Command::new(
            pg_path()
                .join("install")
                .join(pg_install_dir())
                .join("bin")
                .join("psql"),
        )
        .arg("-d")
        .arg("postgres")
        .arg("-p")
        .arg("7999")
        .arg("-c")
        .arg("select 1")
        .output()
        .await
        .unwrap()
        .status
        .success()
    }

    async fn kill_old() {
        let pid_file = pg_path().join("data").join("postmaster.pid");
        if let Ok(file) = tokio::fs::read_to_string(pid_file).await {
            let pid = file.as_str().lines().next().unwrap_or_default();
            tracing::debug!("killing old: {}", pid);
            Command::new("kill")
                .arg(pid)
                .status()
                .await
                .expect("killing old process");
        }
    }

    pub async fn start_postgresql() -> Result<()> {
        download_postgresql().await?;
        kill_old().await;
        for _ in 0..10 {
            if !check_pg().await {
                break;
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                tracing::debug!("waiting for postgres to shutdown");
            }
        }

        tracing::info!("clearing data directory: {:?}", pg_path().join("data"));
        if let Err(e) = fs::remove_dir_all(pg_path().join("data")).await {
            tracing::error!("error removing data dir: {}", e);
        }

        let output = Command::new(
            pg_path()
                .join("install")
                .join(pg_install_dir())
                .join("bin")
                .join("initdb"),
        )
        .arg("-D")
        .arg(pg_path().join("data"))
        .output()
        .await?;
        if !output.status.success() {
            let msg = format!(
                "out:\n{}\nerr:\n{}\n",
                String::from_utf8(output.stdout).unwrap(),
                String::from_utf8(output.stderr).unwrap()
            );
            tracing::error!("initdb: {}", msg);
            return Err(eyre!("initdb error: {}", msg));
        }
        tracing::debug!("initdb: {}", String::from_utf8(output.stdout).unwrap());

        let mut cmd = Command::new(
            pg_path()
                .join("install")
                .join(pg_install_dir())
                .join("bin")
                .join("postgres"),
        )
        .arg("-D")
        .arg(pg_path().join("data"))
        .arg("-p")
        .arg("7999")
        .arg("-c")
        .arg("fsync=off")
        .arg("-c")
        .arg("synchronous_commit=off")
        .arg("-c")
        .arg("full_page_writes=off")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
        let (mut stdout, mut stderr) = (
            BufReader::new(cmd.stdout.take().expect("unable to get postgres stdout")).lines(),
            BufReader::new(cmd.stderr.take().expect("unable to get postgres stdout")).lines(),
        );
        tokio::spawn(async move {
            cmd.wait().await.expect("postgres encountered an error");
        });
        tokio::spawn(async move {
            while let Ok(Some(line)) = stdout.next_line().await {
                tracing::debug!("pg out: {}", line);
            }
        });
        tokio::spawn(async move {
            while let Ok(Some(line)) = stderr.next_line().await {
                tracing::error!("pg err: {}", line);
            }
        });
        for _ in 0..30 {
            if check_pg().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(eyre!("unable to start"))
    }
}

#[cfg(feature = "test")]
#[cfg(test)]
mod tests {
    use crate::pg::test::start_postgresql;

    #[test_log::test(tokio::test)]
    async fn test_start() {
        assert!(start_postgresql().await.is_ok());
    }
}
