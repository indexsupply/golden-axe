use axum::{extract::FromRef, response::Html};
use axum_extra::extract::cookie::Key;
use axum_flash::IncomingFlashes;
use deadpool_postgres::Pool;
use reqwest::StatusCode;
use rust_embed::Embed;
use serde::Serialize;

use crate::{email, stripe};

#[derive(Embed)]
#[folder = "src/docs"]
#[include = "*.md"]
pub struct Docs;

pub fn get_doc_md(path: &str) -> String {
    let file = Docs::get(path).unwrap().data;
    std::str::from_utf8(&file).unwrap().to_string()
}

#[derive(Clone)]
pub struct State {
    pub api_url: String,
    pub flash: axum_flash::Config,
    pub templates: handlebars::Handlebars<'static>,
    pub pool: Pool,
    pub key: Key,
    pub sendgrid: email::Client,
    pub stripe_pub_key: String,
    pub stripe: stripe::Client,
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

#[derive(Debug)]
pub struct Error(pub eyre::Report);

impl From<handlebars::RenderError> for Error {
    fn from(err: handlebars::RenderError) -> Self {
        Error(err.into())
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Error(err.into())
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(err: deadpool_postgres::PoolError) -> Self {
        Error(err.into())
    }
}

impl From<eyre::Report> for Error {
    fn from(value: eyre::Report) -> Self {
        Error(value)
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error(value.into())
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        tracing::error!("error: {}", self.0.to_string());
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!("ouch: {}", self.0)),
        )
            .into_response()
    }
}
