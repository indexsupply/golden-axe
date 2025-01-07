use axum::{extract::State, Json};
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::web;

/*
    Based on this API spec:
    https://conduitxyz.notion.site/External-Native-Integrations-Guide-f69c5ae4df374f6fbc4513c180748e56
*/

#[derive(Deserialize)]
pub struct CreateRequest {
    pub id: String,
    pub event: String,
    pub chain_id: u64,
    pub rpc: String,
}

#[derive(Serialize)]
pub struct CreateResponse {
    pub id: String,
    pub status: String,
}

pub async fn add(
    State(state): State<web::State>,
    Json(req): Json<CreateRequest>,
) -> Result<Json<CreateResponse>, web::Error> {
    let pg = state.pool.get().await?;
    pg.execute(
        "insert into config(enabled, chain, url, conduit_id) values (true, $1, $2, $3)",
        &[&(req.chain_id as i64), &req.rpc, &req.id],
    )
    .await?;
    Ok(Json(CreateResponse {
        id: req.id.clone(),
        status: String::from("INSTALLED"),
    }))
}
