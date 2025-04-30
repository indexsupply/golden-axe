use axum::{extract::State, Json};
use eyre::Context;
use getrandom::getrandom;
use serde::{Deserialize, Serialize};

use crate::web;

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    org: String,
    name: Option<String>,
    hard_limit: bool,
    origins: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct CreateKeyResponse {
    secret: String,
}

pub async fn create_key(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<CreateKeyRequest>,
) -> Result<Json<CreateKeyResponse>, shared::Error> {
    let mut rndbytes = vec![0u8; 16];
    getrandom(&mut rndbytes).wrap_err("unable to generate secret")?;
    let secret = format!("wl{}", hex::encode(rndbytes));
    let pg = state.pool.get().await?;
    pg.execute(
        "
        insert into wl_api_keys(provision_key, org, name, hard_limit, secret, origins)
        values ($1, $2, $3, $4, $5, coalesce($6, '{}'::text[]))
        ",
        &[
            &provision_key.secret,
            &req.org,
            &req.name,
            &req.hard_limit,
            &secret,
            &req.origins,
        ],
    )
    .await
    .map_err(|e| shared::pg::unique_violations(e, &[("unique_api_keys", "key already exists")]))?;
    Ok(Json(CreateKeyResponse { secret }))
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
            select coalesce(sum(n)::int8, 0) as n
            from wl_daily_user_queries
            where provision_key = $3
            and org = $4
            and day >= make_date($1, $2, 1)
            and day < make_date($1, $2, 1) + interval '1 month';
            ",
            &[
                &(req.year as i32),
                &(req.month as i32),
                &provision_key.secret,
                &req.org,
            ],
        )
        .await?
        .iter()
        .map(|row| UsageResponse {
            num_reqs: row.get("n"),
        })
        .next()
        .unwrap_or(UsageResponse { num_reqs: 0 }),
    ))
}

#[derive(Serialize)]
pub struct ListKeysResponse {
    org: String,
    name: Option<String>,
    hard_limit: bool,
    secret: String,
    origins: Vec<String>,
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
                name,
                hard_limit,
                secret,
                origins,
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
            name: row.get("name"),
            hard_limit: row.get("hard_limit"),
            secret: row.get("secret"),
            origins: row.get("origins"),
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

#[derive(Deserialize)]
pub struct UpdateOriginsRequest {
    secret: String,
    origins: Vec<String>,
}

pub async fn update_origins(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<UpdateOriginsRequest>,
) -> Result<(), shared::Error> {
    let pg = state.pool.get().await?;
    let res = pg
        .execute(
            "update wl_api_keys set origins = $3 where provision_key = $1 and secret = $2",
            &[&provision_key.secret, &req.secret, &req.origins],
        )
        .await
        .map_err(|_| shared::Error::User(String::from("unable to update origins")))?;
    if res == 1 {
        Ok(())
    } else {
        Err(shared::Error::User(String::from(
            "unable to update origins",
        )))
    }
}

#[derive(Deserialize)]
pub struct UpdateHardLimitRequest {
    secret: String,
    hard_limit: bool,
}
pub async fn update_hard_limit(
    provision_key: web::ProvisionKey,
    State(state): State<web::State>,
    Json(req): Json<UpdateHardLimitRequest>,
) -> Result<(), shared::Error> {
    let pg = state.pool.get().await?;
    let res = pg
        .execute(
            "update wl_api_keys set hard_limit = $3 where provision_key = $1 and secret = $2",
            &[&provision_key.secret, &req.secret, &req.hard_limit],
        )
        .await
        .map_err(|_| shared::Error::User(String::from("unable to update hard_limit")))?;
    if res == 1 {
        Ok(())
    } else {
        Err(shared::Error::User(String::from(
            "unable to update hard_limit",
        )))
    }
}
