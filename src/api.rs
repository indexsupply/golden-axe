use std::sync::Arc;

use axum::{
    extract::{rejection::JsonRejection, FromRequest},
    http::StatusCode,
};
use eyre::eyre;
use serde::{Deserialize, Serialize};

use deadpool_postgres::Pool;
use serde_json::{json, Value};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Config {
    pub pool: Pool,
    pub broadcaster: Arc<Broadcaster>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    User(String),

    #[serde(skip)]
    Server(Box<dyn std::error::Error + Send + Sync>),
}

impl From<eyre::Report> for Error {
    fn from(value: eyre::Report) -> Self {
        Error::Server(value.into())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Server(err.into())
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        match err.as_db_error() {
            Some(e) => Error::User(e.message().to_string()),
            None => Error::User(err.to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorMessage {
    pub message: String,
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::User(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::Server(e) => {
                tracing::error!(%e, "server-error={:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "server error".to_string(),
                )
            }
        };
        let m = ErrorMessage { message };
        (status, axum::Json(m)).into_response()
    }
}

pub struct Json<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for Json<T>
where
    axum::Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<Value>);

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();
        let req = axum::extract::Request::from_parts(parts, body);

        match axum::Json::<T>::from_request(req, state).await {
            Ok(value) => Ok(Self(value.0)),
            Err(rejection) => Err((
                rejection.status(),
                axum::Json(json!({
                    "message": rejection.body_text(),
                })),
            )),
        }
    }
}

pub async fn client_post<T, U>(
    client: &reqwest::Client,
    url: url::Url,
    request_body: &U,
) -> eyre::Result<T>
where
    T: for<'de> serde::Deserialize<'de>,
    U: serde::Serialize,
{
    let response = client.post(url).json(request_body).send().await?;
    let status = response.status();
    let body = response.text().await?;

    if let Ok(r) = serde_json::from_str::<T>(&body) {
        return Ok(r);
    }
    if let Ok(err) = serde_json::from_str::<ErrorMessage>(&body) {
        return Err(eyre!(err.message));
    }
    if body.is_empty() {
        return Err(eyre!("status: {}", status));
    }
    Err(eyre!("status: {} body:\n{}", status, body))
}

pub struct Broadcaster {
    clients: broadcast::Sender<u64>,
}

impl Broadcaster {
    pub fn new() -> Arc<Broadcaster> {
        let (tx, _) = broadcast::channel(16);
        Arc::new(Broadcaster { clients: tx })
    }
    pub fn add(&self) -> broadcast::Receiver<u64> {
        self.clients.subscribe()
    }

    pub fn broadcast(&self, block: u64) {
        let _ = self.clients.send(block);
    }
}
