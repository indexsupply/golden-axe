use axum::{extract::State, Json};
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::web;

/*
    Based on this API spec:
    https://conduitxyz.notion.site/External-Native-Integrations-Guide-f69c5ae4df374f6fbc4513c180748e56
*/

#[derive(Deserialize, Serialize)]
pub struct CreateRequest {
    pub id: String,
    pub event: String,
    pub chain_id: u64,
    pub rpc: String,
}

#[derive(Deserialize, Serialize)]
pub struct CreateResponse {
    pub id: String,
    pub status: String,
}

pub async fn add(
    State(state): State<web::State>,
    Json(req): Json<CreateRequest>,
) -> Result<Json<CreateResponse>, shared::Error> {
    let pg = state.pool.get().await?;
    pg.execute(
        "insert into config(enabled, chain, url, conduit_id) values (true, $1, $2, $3)",
        &[&(req.chain_id as i64), &req.rpc, &req.id],
    )
    .await
    .map_err(|e| {
        shared::pg::unique_violations(
            e,
            &[
                (
                    "config_pkey",
                    &format!("duplicate for chain: {}", req.chain_id),
                ),
                (
                    "config_conduit_id_key",
                    &format!("duplicate for id: {}", req.id),
                ),
            ],
        )
    })?;
    Ok(Json(CreateResponse {
        id: req.id.clone(),
        status: String::from("INSTALLED"),
    }))
}
