use serde::{Deserialize, Serialize};

use crate::web;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Query {
    pub chain: u64,
    pub desc: Option<String>,
    pub sql: String,
    pub events: Vec<String>,
    pub latency: Option<u64>,

    #[serde(skip_deserializing, with = "time::serde::rfc2822::option")]
    pub created_at: Option<time::OffsetDateTime>,
}

pub async fn user_history(
    pg: &tokio_postgres::Client,
    owner_email: &str,
) -> Result<Vec<Query>, web::Error> {
    Ok(pg
        .query(
            "
            select chain, events, user_query, latency, created_at
            from user_queries
            where api_key in (select secret from api_keys where owner_email = $1)
            order by created_at desc
            limit 100
            ",
            &[&owner_email],
        )
        .await?
        .into_iter()
        .map(|row| Query {
            chain: row.get::<&str, i64>("chain") as u64,
            desc: None,
            events: row.get("events"),
            sql: row.get("user_query"),
            latency: Some(row.get::<&str, i32>("latency") as u64),
            created_at: row.get("created_at"),
        })
        .collect())
}
