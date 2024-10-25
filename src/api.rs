use std::{
    collections::HashMap,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, FromRequest, FromRequestParts},
    http::StatusCode,
};
use eyre::eyre;
use serde::{Deserialize, Serialize};

use deadpool_postgres::Pool;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use url::Url;

use crate::gafe;

pub async fn handle_service_error(error: tower::BoxError) -> Error {
    if error.is::<tower::load_shed::error::Overloaded>() {
        Error::Server(eyre!("server is overloaded").into())
    } else {
        Error::Server(eyre!("unknown").into())
    }
}

#[derive(Clone)]
pub struct Config {
    pub chain_id: u64,
    pub pool: Pool,
    pub broadcaster: Arc<Broadcaster>,
    pub open_limit: Arc<gafe::AccountLimit>,
    pub free_limit: Arc<gafe::AccountLimit>,
    pub account_limits: Arc<Mutex<HashMap<String, Arc<gafe::AccountLimit>>>>,
    pub gafe: gafe::Connection,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    User(String),
    Timeout(Option<String>),
    TooManyRequests(Option<String>),

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
            Self::Timeout(msg) => (
                StatusCode::REQUEST_TIMEOUT,
                msg.unwrap_or(String::from("request timed out")),
            ),
            Self::TooManyRequests(msg) => (
                StatusCode::TOO_MANY_REQUESTS,
                msg.unwrap_or(String::from("too many requests")),
            ),
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

pub async fn limit(
    ConnectInfo(conn_info): ConnectInfo<SocketAddr>,
    origin_domain: Option<OriginDomain>,
    account_limit: Arc<gafe::AccountLimit>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    if !account_limit.origins.is_empty() {
        match origin_domain {
            None => tracing::error!("missing origin"),
            Some(domain) => {
                if !account_limit.origins.contains(&domain) {
                    tracing::error!("origin {} not allowed", domain);
                }
            }
        }
    }
    if account_limit
        .rate
        .check_key(&conn_info.ip().to_string())
        .is_err()
    {
        return Err(Error::TooManyRequests(Some(String::from(
            "Rate limited. Create or upgrade API Key at: https://www.indexsupply.net",
        ))));
    }
    match tokio::time::timeout(account_limit.timeout, next.run(request)).await {
        Ok(response) => Ok(response),
        Err(_) => Err(Error::Timeout(None)),
    }
}

#[derive(Clone)]
pub struct Key(String);

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[axum::async_trait]
impl FromRequestParts<Config> for Key {
    type Rejection = Error;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _: &Config,
    ) -> Result<Self, Self::Rejection> {
        let params = parts.uri.query().unwrap_or_default();
        let decoded =
            serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
        let key = decoded.get("api-key").cloned().unwrap_or_default();
        let short_key = &key[..key.len().min(4)];
        tracing::Span::current().record("api-key", short_key);
        Ok(Key(key))
    }
}

type OriginDomain = String;

#[axum::async_trait]
impl FromRequestParts<Config> for Option<OriginDomain> {
    type Rejection = Error;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _: &Config,
    ) -> Result<Self, Self::Rejection> {
        if let Some(origin_header) = parts.headers.get("origin") {
            if let Ok(origin) = origin_header.to_str() {
                if let Ok(origin) = Url::parse(origin) {
                    if let Some(domain) = origin.domain() {
                        return Ok(Some(OriginDomain::from(domain)));
                    }
                }
            }
        }
        return Ok(None);
    }
}

#[axum::async_trait]
impl FromRequestParts<Config> for Arc<gafe::AccountLimit> {
    type Rejection = Error;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        config: &Config,
    ) -> Result<Self, Self::Rejection> {
        if !config.gafe.live().await {
            return Ok(config.open_limit.clone());
        }
        let params = parts.uri.query().unwrap_or_default();
        let decoded =
            serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
        let client_id = decoded.get("api-key").cloned().unwrap_or_default();
        let client_id_short = &client_id[..client_id.len().min(4)];
        tracing::Span::current().record("api-key", client_id_short);
        match config.account_limits.lock().unwrap().get(&client_id) {
            Some(limit) => Ok(limit.clone()),
            None => Ok(config.free_limit.clone()),
        }
    }
}

pub async fn latency_header(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    let start = tokio::time::Instant::now();
    let mut response = next.run(request).await;
    let duration = start.elapsed().as_millis();
    let latency = format!("{:.2?}ms", duration);
    response.headers_mut().insert(
        "Latency",
        axum::http::HeaderValue::from_str(&latency).unwrap(),
    );
    Ok(response)
}
