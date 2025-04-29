use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, FromRef, FromRequestParts},
    response::{Html, IntoResponse},
};
use axum_extra::extract::cookie::Key;
use axum_flash::IncomingFlashes;
use deadpool_postgres::Pool;
use serde::Serialize;
use serde_json::json;

use crate::{chains, daimo, postmark, query, stripe};

#[derive(Clone)]
pub struct State {
    pub admin_api_secret: String,
    pub be_url: String,
    pub fe_url: String,
    pub flash: axum_flash::Config,
    pub templates: handlebars::Handlebars<'static>,
    pub pool: Pool,
    pub key: Key,
    pub postmark: postmark::Client,
    pub stripe_pub_key: Option<String>,
    pub stripe: stripe::Client,
    pub examples: Vec<query::Query>,
    pub daimo: daimo::Client,
}

impl FromRef<State> for Key {
    fn from_ref(state: &State) -> Self {
        state.key.clone()
    }
}

impl FromRef<State> for axum_flash::Config {
    fn from_ref(state: &State) -> axum_flash::Config {
        state.flash.clone()
    }
}

#[derive(Serialize)]
pub struct FlashMessage {
    pub level: String,
    pub message: String,
}

impl FlashMessage {
    pub fn from(flashes: IncomingFlashes) -> Vec<Self> {
        flashes
            .into_iter()
            .map(|f| FlashMessage {
                level: format!("{:?}", f.0),
                message: f.1.to_string(),
            })
            .collect()
    }
}

pub struct ProvisionKey {
    pub secret: String,
}

#[axum::async_trait]
impl FromRequestParts<State> for ProvisionKey {
    type Rejection = shared::Error;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &State,
    ) -> Result<Self, Self::Rejection> {
        if let Some(addr) = parts.extensions.get::<ConnectInfo<SocketAddr>>() {
            if addr.ip().is_loopback() {
                return Ok(ProvisionKey {
                    secret: String::from("localhost"),
                });
            }
        }
        let header = parts
            .headers
            .get("authorization")
            .ok_or(shared::Error::Authorization(String::from(
                "missing auth header",
            )))?
            .to_str()
            .map_err(|_| shared::Error::Authorization(String::from("invalid auth header")))?;
        let creds = http_basic_auth::decode(header).map_err(|_| {
            shared::Error::Authorization(String::from("unable to parse basic auth"))
        })?;
        match state
            .pool
            .get()
            .await?
            .query_one(
                "select true from provision_keys where secret = $1 and deleted_at is null",
                &[&creds.user_id],
            )
            .await
        {
            Ok(_) => Ok(ProvisionKey {
                secret: creds.user_id.to_string(),
            }),
            _ => Err(shared::Error::Authorization(String::from(
                "unable to auth provision endpoint",
            ))),
        }
    }
}

pub async fn status(
    axum::extract::State(state): axum::extract::State<State>,
) -> Result<impl IntoResponse, shared::Error> {
    let pg = state.pool.get().await?;
    let chains = chains::list(&pg)
        .await?
        .into_iter()
        .filter(|c| c.enabled)
        .collect::<Vec<_>>();
    Ok(Html(state.templates.render(
        "status.html",
        &json!({"api_url": state.be_url, "chains": chains}),
    )?))
}
