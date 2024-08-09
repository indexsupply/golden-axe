use axum::extract::FromRef;
use axum_extra::extract::cookie::Key;
use deadpool_postgres::Pool;
use maud::html;
use reqwest::StatusCode;

use crate::email;

#[derive(Clone)]
pub struct State {
    pub flash: axum_flash::Config,
    pub pool: Pool,
    pub key: Key,
    pub sendgrid: email::Client,
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

#[derive(Debug)]
pub struct Error(pub eyre::Report);

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
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            html! {
                body {
                    p { "ouch! " (self.0.to_string())}
                }
            },
        )
            .into_response()
    }
}
