use eyre::Context;
use getrandom::getrandom;
use serde::{Deserialize, Deserializer, Serialize};
use tokio_postgres::Client;

time::serde::format_description!(
    short,
    OffsetDateTime,
    "[year]-[month]-[day] [hour]:[minute]:[second]"
);

#[derive(Clone, Debug, Serialize)]
pub struct ApiKey {
    secret: String,
    origins: Vec<String>,
    ip_connections: Option<i32>,
    #[serde(skip_deserializing, with = "short")]
    created_at: time::OffsetDateTime,
}

pub async fn delete(pg: &Client, owner_email: &str, secret: String) -> Result<(), shared::Error> {
    pg.query(
        "update api_keys set deleted_at = now() where owner_email = $1 and secret = $2",
        &[&owner_email, &secret],
    )
    .await?;
    Ok(())
}

pub async fn create(
    pg: &Client,
    owner_email: &str,
    origins: String,
    ip_connections: Option<i32>,
) -> Result<(), shared::Error> {
    let mut secret = vec![0u8; 16];
    getrandom(&mut secret).wrap_err("unable to generate secret")?;
    let origins = if origins.is_empty() {
        Vec::new()
    } else {
        origins.split(",").map(String::from).collect()
    };
    pg.query(
        "insert into api_keys(owner_email, secret, origins, ip_connections) values ($1, $2, $3, $4)",
        &[&owner_email, &hex::encode(secret), &origins, &ip_connections],
    )
    .await?;
    Ok(())
}

pub async fn list(pg: &Client, owner_email: &str) -> Result<Vec<ApiKey>, shared::Error> {
    let res = pg
        .query(
            "
            select secret, origins, ip_connections, created_at
            from api_keys
            where owner_email = $1
            and deleted_at is null
            order by created_at desc
            ",
            &[&owner_email],
        )
        .await?;
    Ok(res
        .iter()
        .map(|row| ApiKey {
            secret: row.get("secret"),
            origins: row.get("origins"),
            ip_connections: row.get("ip_connections"),
            created_at: row.get("created_at"),
        })
        .collect::<Vec<ApiKey>>())
}

pub mod handlers {
    use axum::{
        extract::State,
        response::{Html, IntoResponse, Redirect},
        Form, Json,
    };
    use serde::Deserialize;
    use serde_json::json;

    use crate::{account, session, web};

    pub async fn new(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        user: session::User,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        if let Some(plan) = account::PlanChange::get_latest_completed(&pg, &user.email).await? {
            let rendered_html = state.templates.render(
                "new-api-key.html",
                &json!({
                    "user": user,
                    "plan": plan,
                }),
            )?;
            Ok((Html(rendered_html)).into_response())
        } else {
            let flash = flash.error("Paid plan required for API keys");
            Ok((flash, Redirect::to("/account")).into_response())
        }
    }

    #[derive(Deserialize)]
    pub struct NewKeyRequest {
        origins: String,
        #[serde(deserialize_with = "super::empty_string_as_none")]
        ip_connections: Option<i32>,
    }
    pub async fn create(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        user: session::User,
        Form(req): Form<NewKeyRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        super::create(&pg, &user.email, req.origins, req.ip_connections).await?;
        let flash = flash.success("api key created");
        Ok((flash, Redirect::to("/account")))
    }

    #[derive(Deserialize)]
    pub struct EditKeyRequest {
        secret: String,
    }

    pub async fn edit(
        State(state): State<web::State>,
        user: session::User,
        Form(req): Form<EditKeyRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let plan = account::PlanChange::get_latest_completed(&pg, &user.email).await?;
        let row = pg
            .query_one(
                "select origins, ip_connections from api_keys where owner_email = $1 and secret = $2",
                &[&user.email, &req.secret]
            )
            .await?;
        let origins: Vec<String> = row.get("origins");
        let ip_connections: Option<i32> = row.get("ip_connections");
        let rendered_html = state.templates.render(
            "edit-api-key.html",
            &json!({
                "plan": plan,
                "secret": req.secret,
                "origins": origins,
                "ip_connections": ip_connections,
            }),
        )?;
        Ok((Html(rendered_html)).into_response())
    }

    #[derive(Deserialize)]
    pub struct UpdateKeyRequest {
        secret: String,
        origins: String,
        #[serde(deserialize_with = "super::empty_string_as_none")]
        ip_connections: Option<i32>,
    }

    pub async fn update(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        user: session::User,
        Form(req): Form<UpdateKeyRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let origins = if req.origins.is_empty() {
            Vec::new()
        } else {
            req.origins.split(",").map(String::from).collect()
        };
        let ip_connections = match req.ip_connections {
            Some(0) => None,
            Some(n) => Some(n),
            None => None,
        };
        let res = pg
            .execute(
                "
                update api_keys
                set origins = $1, ip_connections = $2
                where owner_email = $3 and secret = $4
                ",
                &[&origins, &ip_connections, &user.email, &req.secret],
            )
            .await?;
        if res != 1 {
            let flash = flash.error("updating key FAILED");
            Ok((flash, Redirect::to("/account")).into_response())
        } else {
            let flash = flash.success("api key updated");
            Ok((flash, Redirect::to("/account")).into_response())
        }
    }

    pub async fn delete(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        user: session::User,
        Json(secret): Json<String>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        super::delete(&pg, &user.email, secret).await?;
        let flash = flash.success("api key deleted");
        Ok((flash, axum::http::StatusCode::OK).into_response())
    }
}

fn empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    Ok(s.and_then(|s| {
        if s.trim().is_empty() {
            None
        } else {
            s.parse().ok()
        }
    }))
}
