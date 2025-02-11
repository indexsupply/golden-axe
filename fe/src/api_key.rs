use eyre::Context;
use getrandom::getrandom;
use serde::Serialize;
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

pub async fn create(pg: &Client, owner_email: &str, origins: String) -> Result<(), shared::Error> {
    let mut secret = vec![0u8; 16];
    getrandom(&mut secret).wrap_err("unable to generate secret")?;
    let origins = if origins.is_empty() {
        Vec::new()
    } else {
        origins.split(",").map(String::from).collect()
    };
    pg.query(
        "insert into api_keys(owner_email, secret, origins) values ($1, $2, $3)",
        &[&owner_email, &hex::encode(secret), &origins],
    )
    .await?;
    Ok(())
}

pub async fn list(pg: &Client, owner_email: &str) -> Result<Vec<ApiKey>, shared::Error> {
    let res = pg
        .query(
            "
            select secret, origins, created_at
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
    use axum_extra::extract::SignedCookieJar;
    use serde::Deserialize;
    use serde_json::json;

    use crate::{account, session, web};

    pub async fn new(
        flash: axum_flash::Flash,
        State(state): State<web::State>,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, shared::Error> {
        let user = session::User::from_jar(jar).unwrap();
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
    }
    pub async fn create(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        jar: SignedCookieJar,
        Form(req): Form<NewKeyRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let pg = state.pool.get().await?;
        super::create(&pg, &user.email, req.origins).await?;
        let flash = flash.success("api key created");
        Ok((flash, Redirect::to("/account")))
    }

    pub async fn delete(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        jar: SignedCookieJar,
        Json(secret): Json<String>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let pg = state.pool.get().await?;
        super::delete(&pg, &user.email, secret).await?;
        let flash = flash.success("api key deleted");
        Ok((flash, axum::http::StatusCode::OK).into_response())
    }
}
