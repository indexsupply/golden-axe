use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::web;

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    org: String,
    secret: String,
    origins: Option<Vec<String>>,
}

pub async fn create_key(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<CreateKeyRequest>,
) -> Result<(), shared::Error> {
    let pg = state.pool.get().await?;
    pg.execute(
        "insert into wl_api_keys(provision_key, org, secret, origins) values ($1, $2, $3, $4)",
        &[&provision_key.secret, &req.org, &req.secret, &req.origins],
    )
    .await
    .map_err(|e| shared::pg::unique_violations(e, &[("unique_api_keys", "key already exists")]))?;
    Ok(())
}

#[derive(Deserialize)]
pub struct UsageRequest {
    org: String,
    month: u16,
    year: u16,
}

#[derive(Serialize)]
pub struct UsageResponse {
    num_reqs: i64,
}

pub async fn usage(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<UsageRequest>,
) -> Result<Json<UsageResponse>, shared::Error> {
    let pg = state.pool.get().await?;
    Ok(Json(
        pg.query(
            "
            select count(*)::int8 as num_reqs
            from user_queries
            where api_key in (select secret from wl_api_keys where provision_key = $1 and org = $2)
            and date_part('year', created_at)::int8 = $3
            and date_part('month', created_at)::int8 = $4
        ",
            &[
                &provision_key.secret,
                &req.org,
                &(req.year as i64),
                &(req.month as i64),
            ],
        )
        .await?
        .iter()
        .map(|row| UsageResponse {
            num_reqs: row.get("num_reqs"),
        })
        .next()
        .unwrap_or(UsageResponse { num_reqs: 0 }),
    ))
}

#[derive(Serialize)]
pub struct ListKeysResponse {
    org: String,
    secret: String,
    created_at: i64,
    deleted_at: Option<i64>,
}

#[derive(Deserialize)]
pub struct ListKeysRequest {
    org: String,
}

pub async fn list_keys(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<ListKeysRequest>,
) -> Result<Json<Vec<ListKeysResponse>>, shared::Error> {
    let pg = state.pool.get().await?;
    Ok(Json(
        pg.query(
            "
            select
                org,
                secret,
                extract(epoch from created_at)::int8 as created_at,
                extract(epoch from deleted_at)::int8 as deleted_at
            from wl_api_keys
            where provision_key = $1
            and org = $2
        ",
            &[&provision_key.secret, &req.org],
        )
        .await?
        .iter()
        .map(|row| ListKeysResponse {
            org: row.get("org"),
            secret: row.get("secret"),
            created_at: row.get("created_at"),
            deleted_at: row.get("deleted_at"),
        })
        .collect::<Vec<_>>(),
    ))
}

#[derive(Deserialize)]
pub struct DeleteKeyRequest {
    secret: String,
}

pub async fn delete_key(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<DeleteKeyRequest>,
) -> Result<(), shared::Error> {
    let pg = state.pool.get().await?;
    let res = pg
        .execute(
            "update wl_api_keys set deleted_at = now() where provision_key = $1 and secret = $2",
            &[&provision_key.secret, &req.secret],
        )
        .await
        .map_err(|_| shared::Error::User(String::from("unable to delete key")))?;
    if res == 1 {
        Ok(())
    } else {
        Err(shared::Error::User(String::from("unable to delete key")))
    }
}
