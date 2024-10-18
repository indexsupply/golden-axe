use crate::web::Error;
use crate::{session, web};
use axum::extract::{FromRequestParts, State};
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
    events: Vec<String>,
    sql: String,
    latency: u64,
    #[serde(skip_deserializing, with = "short")]
    created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Request {
    owner_email: Option<String>,
}

impl Request {
    fn sql(&self) -> String {
        let mut predicates = Vec::new();
        if let Some(email) = &self.owner_email {
            predicates.push(format!("owner_email like '%{}%'", email))
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
    let history = pg
        .query(
            &format!(
                "
                select
                    coalesce(nullif(owner_email, ''), 'free') owner_email,
                    events,
                    user_query,
                    latency,
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
            owner_email: row.get("owner_email"),
            events: row.get("events"),
            sql: row.get("user_query"),
            latency: row.get::<_, i32>("latency") as u64,
            created_at: row.get("created_at"),
        })
        .collect::<Vec<UserQuery>>();
    Ok(Html(
        state
            .templates
            .render("godmode", &json!({"history": history}))?,
    ))
}

pub struct God {}

#[axum::async_trait]
impl FromRequestParts<web::State> for God {
    type Rejection = Error;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &web::State,
    ) -> Result<Self, Self::Rejection> {
        let jar: SignedCookieJar = SignedCookieJar::from_request_parts(parts, state)
            .await
            .unwrap();
        match session::User::from_jar(jar) {
            Some(user) if user.email == "r@32k.io" => Ok(God {}),
            _ => Err(web::Error(eyre!("not authorized"))),
        }
    }
}
