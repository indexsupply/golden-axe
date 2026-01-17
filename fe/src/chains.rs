use eyre::Result;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub name: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub popular: bool,
    #[serde(default)]
    pub hidden: bool,
    pub chain: i64,
    pub start_block: Option<i64>,
    #[serde(skip_serializing)]
    pub url: String,
}

fn default_enabled() -> bool {
    true
}

#[derive(Deserialize)]
pub struct EnableRequest {
    pub chain: i64,
}

pub mod handlers {
    use super::{Config, EnableRequest};
    use crate::web;
    use axum::{extract::State, Json};
    use be::sync;
    use rust_decimal::prelude::One;

    pub async fn enable(
        provision_key: web::ProvisionKey,
        State(state): State<web::State>,
        Json(req): Json<EnableRequest>,
    ) -> Result<(), shared::Error> {
        let pg = state.pool.get().await?;
        let res = pg
            .execute(
                "update config set enabled = true where chain = $1 and provision_key = $2",
                &[&req.chain, &provision_key.secret],
            )
            .await?;
        if res.is_one() {
            Ok(())
        } else {
            Err(shared::Error::User(format!(
                "unable to enable chain {}",
                req.chain,
            )))
        }
    }

    pub async fn disable(
        provision_key: web::ProvisionKey,
        State(state): State<web::State>,
        Json(req): Json<EnableRequest>,
    ) -> Result<(), shared::Error> {
        let pg = state.pool.get().await?;
        let res = pg
            .execute(
                "update config set enabled = false where chain = $1 and provision_key = $2",
                &[&req.chain, &provision_key.secret],
            )
            .await?;
        if res.is_one() {
            Ok(())
        } else {
            Err(shared::Error::User(format!(
                "unable to disable chain {}",
                req.chain,
            )))
        }
    }

    pub async fn add(
        provision_key: web::ProvisionKey,
        State(state): State<web::State>,
        Json(req): Json<Config>,
    ) -> Result<(), shared::Error> {
        sync::test(&req.url, req.chain as u64).await?;
        let pg = state.pool.get().await?;
        pg.execute(
            "
            insert into config(enabled, name, chain, url, start_block, provision_key)
            values (true, $1, $2, $3, $4, $5)
            ",
            &[
                &req.name,
                &req.chain,
                &req.url,
                &req.start_block,
                &provision_key.secret,
            ],
        )
        .await
        .map_err(|e| {
            shared::pg::unique_violations(
                e,
                &[(
                    "config_pkey",
                    &format!("duplicate for chain: {}", req.chain),
                )],
            )
        })?;
        Ok(())
    }

    pub async fn list(State(state): State<web::State>) -> Result<Json<Vec<Config>>, shared::Error> {
        let pg = state.pool.get().await?;
        Ok(Json(super::list(&pg).await?))
    }
}

pub async fn list(pg: &tokio_postgres::Client) -> Result<Vec<Config>> {
    Ok(pg
        .query(
            "select enabled, chain, name, url, start_block, popular, hidden from config order by chain",
            &[],
        )
        .await?
        .iter()
        .map(|row| Config {
            name: row.get("name"),
            enabled: row.get("enabled"),
            popular: row.get("popular"),
            hidden: row.get("hidden"),
            chain: row.get("chain"),
            url: row.get("url"),
            start_block: row.get("start_block"),
        })
        .collect())
}
