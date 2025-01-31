use std::net::SocketAddr;

use crate::web::Error;
use crate::{session, web};
use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::response::{Html, IntoResponse};
use axum::Form;
use axum_extra::extract::SignedCookieJar;
use eyre::{eyre, Context};
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;

time::serde::format_description!(
    short,
    OffsetDateTime,
    "[year]-[month]-[day] [hour]:[minute]:[second]"
);

#[derive(Clone, Debug, Serialize)]
struct UserQuery {
    owner_email: Option<String>,
    chain: be::api::Chain,
    events: Vec<String>,
    sql: String,
    latency: Option<u64>,
    status: Option<u16>,
    count: Option<u64>,
    #[serde(skip_deserializing, with = "short")]
    created_at: OffsetDateTime,
    #[serde(skip_deserializing)]
    generated_sql: Option<String>,
}

impl UserQuery {
    pub fn gen_sql(mut self) -> UserQuery {
        self.generated_sql = be::query::sql(
            self.chain,
            None,
            &self.sql,
            self.events.iter().map(AsRef::as_ref).collect(),
        )
        .ok();
        self
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Request {
    top: Option<bool>,
    status: Option<u16>,
    owner_email: Option<String>,
}

impl Request {
    fn sql(&self) -> String {
        let mut predicates = vec![String::from(
            "user_queries.created_at > now() - '1 day'::interval",
        )];
        if let Some(email) = &self.owner_email {
            predicates.push(format!("owner_email like '%{}%'", email))
        }
        if let Some(status) = &self.status {
            predicates.push(format!("status / 100 = {}", status))
        }
        if predicates.is_empty() {
            String::new()
        } else {
            format!("where {}", predicates.join(" and "))
        }
    }
}

pub async fn index(
    _: God,
    State(state): State<web::State>,
    Form(req): Form<Request>,
) -> Result<impl IntoResponse, web::Error> {
    let pg = state.pool.get().await.wrap_err("getting db connection")?;
    let history = if req.top.is_some() {
        top(&pg, Form(req)).await?
    } else {
        log(&pg, Form(req)).await?
    };
    Ok(Html(
        state
            .templates
            .render("godmode.html", &json!({"history": history}))?,
    ))
}

async fn log(
    pg: &tokio_postgres::Client,
    Form(req): Form<Request>,
) -> Result<Vec<UserQuery>, web::Error> {
    Ok(pg
        .query(
            &format!(
                "
                select
                    chain,
                    coalesce(nullif(owner_email, ''), 'free') owner_email,
                    events,
                    user_query,
                    latency,
                    status,
                    user_queries.created_at
                from user_queries
                left join api_keys on api_keys.secret = user_queries.api_key
                {}
                order by user_queries.created_at desc
                limit 100
                ",
                req.sql()
            ),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| UserQuery {
            chain: be::api::Chain(row.get::<&str, i64>("chain") as u64),
            owner_email: row.get("owner_email"),
            events: row.get("events"),
            sql: row.get("user_query"),
            generated_sql: None,
            count: None,
            latency: Some(row.get::<_, i32>("latency") as u64),
            status: Some(row.get::<_, i16>("status") as u16),
            created_at: row.get("created_at"),
        })
        .map(UserQuery::gen_sql)
        .collect::<Vec<UserQuery>>())
}

async fn top(
    pg: &tokio_postgres::Client,
    Form(req): Form<Request>,
) -> Result<Vec<UserQuery>, web::Error> {
    Ok(pg
        .query(
            &format!(
                r#"
                select
                    chain,
                    coalesce(nullif(owner_email, ''), 'free') owner_email,
                    events,
                    coalesce(substring(
                        regexp_replace(lower(user_query), '[\s\n\t]+', ' ', 'g')
                        from
                        '^(.+?(?= where ))'
                    ), regexp_replace(lower(user_query), '[\s\n\t]+', ' ', 'g') ) as user_query,
                    count(*) count,
                    max(latency) latency,
                    max(user_queries.created_at) created_at
                from user_queries
                left join api_keys on api_keys.secret = user_queries.api_key
                {}
                group by 1, 2, 3
                order by count desc, created_at desc
                "#,
                req.sql()
            ),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| UserQuery {
            chain: be::api::Chain(row.get::<&str, i64>("chain") as u64),
            owner_email: row.get("owner_email"),
            events: row.get("events"),
            sql: row.get("user_query"),
            generated_sql: None,
            count: Some(row.get::<_, i64>("count") as u64),
            latency: Some(row.get::<_, i32>("latency") as u64),
            status: None,
            created_at: row.get("created_at"),
        })
        .collect::<Vec<UserQuery>>())
}

pub struct God {}

#[axum::async_trait]
impl FromRequestParts<web::State> for God {
    type Rejection = Error;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &web::State,
    ) -> Result<Self, Self::Rejection> {
        if let Some(addr) = parts.extensions.get::<ConnectInfo<SocketAddr>>() {
            if addr.ip().is_loopback() {
                return Ok(God {});
            }
        }
        let jar: SignedCookieJar = SignedCookieJar::from_request_parts(parts, state)
            .await
            .unwrap();
        match session::User::from_jar(jar) {
            Some(user) if user.email == "r@32k.io" => Ok(God {}),
            _ => Err(web::Error(eyre!("not authorized"))),
        }
    }
}
