use std::time::SystemTime;

use eyre::Context;
use getrandom::getrandom;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use tokio_postgres::Client;

use crate::web;

pub struct ApiKey {
    secret: Vec<u8>,
    origins: Vec<String>,
    created_at: SystemTime,
}

impl Serialize for ApiKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ApiKey", 3)?;
        state.serialize_field("secret", &hex::encode(&self.secret))?;
        state.serialize_field(
            "created_at",
            &humantime::format_rfc3339_seconds(self.created_at).to_string(),
        )?;
        state.serialize_field("origins", &self.origins.join(","))?;
        state.end()
    }
}

pub async fn delete(pg: &Client, owner_email: &str, secret: Vec<u8>) -> Result<(), web::Error> {
    pg.query(
        "update api_keys set deleted_at = now() where owner_email = $1 and secret = $2",
        &[&owner_email, &secret],
    )
    .await?;
    Ok(())
}

pub async fn create(pg: &Client, owner_email: &str, origins: String) -> Result<(), web::Error> {
    let mut secret = vec![0u8; 16];
    getrandom(&mut secret).wrap_err("unable to generate secret")?;
    let origins = if origins.is_empty() {
        Vec::new()
    } else {
        origins.split(",").map(String::from).collect()
    };
    pg.query(
        "insert into api_keys(owner_email, secret, origins) values ($1, $2, $3)",
        &[&owner_email, &secret, &origins],
    )
    .await?;
    Ok(())
}

pub async fn list(pg: &Client, owner_email: &str) -> Result<Vec<ApiKey>, web::Error> {
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
    use eyre::Context;
    use serde::Deserialize;
    use serde_json::json;

    use crate::{account, session, web};

    use super::list;

    pub async fn new(
        State(state): State<web::State>,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, web::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let pg = state.pool.get().await?;
        let plan = account::current_plan(&pg, &user.email).await?;
        let api_keys = list(&pg, &user.email).await?;
        let rendered_html = state.templates.render(
            "new-api-key",
            &json!({
                "user": user,
                "plan": plan,
                "api_keys": api_keys,
            }),
        )?;
        Ok((Html(rendered_html)).into_response())
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
    ) -> Result<impl IntoResponse, web::Error> {
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
    ) -> Result<impl IntoResponse, web::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let pg = state.pool.get().await?;
        let secret = hex::decode(secret).wrap_err("unable to hex decode secret")?;
        super::delete(&pg, &user.email, secret).await?;
        let flash = flash.success("endpoint deleted");
        Ok((flash, axum::http::StatusCode::OK).into_response())
    }
}
