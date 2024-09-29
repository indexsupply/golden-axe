use eyre::Context;
use getrandom::getrandom;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use tokio_postgres::Client;

use crate::web;

pub struct ApiKey {
    secret: Vec<u8>,
}

impl Serialize for ApiKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Endpoint", 2)?;
        state.serialize_field("url", &url(&self.secret))?;
        state.end()
    }
}

pub fn url(secret: &Vec<u8>) -> String {
    format!(
        "https://api.indexsupply.com?api-key={}",
        hex::encode(secret)
    )
}

pub async fn delete(pg: &Client, owner_email: &str, secret: Vec<u8>) -> Result<(), web::Error> {
    pg.query(
        "update api_keys set deleted_at = now() where owner_email = $1 and secret = $2",
        &[&owner_email, &secret],
    )
    .await?;
    Ok(())
}

pub async fn create(pg: &Client, owner_email: &str) -> Result<(), web::Error> {
    let mut secret = vec![0u8; 16];
    getrandom(&mut secret).wrap_err("unable to generate secret")?;
    pg.query(
        "insert into api_keys(owner_email, secret) values ($1, $2)",
        &[&owner_email, &secret],
    )
    .await?;
    Ok(())
}

pub async fn list(pg: &Client, owner_email: &str) -> Result<Vec<ApiKey>, web::Error> {
    let res = pg
        .query(
            "
            select secret
            from api_keys
            where owner_email = $1
            and deleted_at is null
            ",
            &[&owner_email],
        )
        .await?;
    Ok(res
        .iter()
        .map(|row| ApiKey { secret: row.get(0) })
        .collect::<Vec<ApiKey>>())
}
