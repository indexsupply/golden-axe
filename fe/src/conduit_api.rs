use axum::{extract::State, Json};
use be::sync;
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::web;

#[derive(Deserialize, Serialize)]
pub struct CreateRequest {
    pub name: String,
    pub chain_id: u64,
    pub url: String,
}

#[derive(Deserialize, Serialize)]
pub struct CreateResponse {
    pub id: u64,
}

pub async fn add(
    State(state): State<web::State>,
    Json(req): Json<CreateRequest>,
) -> Result<Json<CreateResponse>, shared::Error> {
    sync::test(&req.url, req.chain_id).await?;
    let pg = state.pool.get().await?;
    pg.execute(
        "insert into config(enabled, name, chain, url) values (true, $1, $2, $3)",
        &[&req.name, &(req.chain_id as i64), &req.url],
    )
    .await
    .map_err(|e| {
        shared::pg::unique_violations(
            e,
            &[(
                "config_pkey",
                &format!("duplicate for chain: {}", req.chain_id),
            )],
        )
    })?;
    Ok(Json(CreateResponse { id: req.chain_id }))
}
