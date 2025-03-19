use eyre::Result;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub name: String,
    #[serde(default, skip_serializing)]
    pub popular: bool,
    pub chain: u64,
    #[serde(skip_serializing)]
    pub url: String,
}

pub mod handlers {
    use super::Config;
    use crate::web;
    use axum::{extract::State, Json};
    use be::sync;

    pub async fn add(
        _: web::Provision,
        State(state): State<web::State>,
        Json(req): Json<Config>,
    ) -> Result<(), shared::Error> {
        sync::test(&req.url, req.chain).await?;
        let pg = state.pool.get().await?;
        pg.execute(
            "insert into config(enabled, name, chain, url) values (true, $1, $2, $3)",
            &[&req.name, &(req.chain as i64), &req.url],
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
            "select chain, name, url, popular from config where enabled order by chain",
            &[],
        )
        .await?
        .iter()
        .map(|row| Config {
            name: row.get("name"),
            popular: row.get("popular"),
            chain: row.get::<&str, i64>("chain") as u64,
            url: row.get("url"),
        })
        .collect())
}
