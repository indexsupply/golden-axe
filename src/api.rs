use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, FromRequest, FromRequestParts},
    http::StatusCode,
};
use eyre::eyre;
use governor::{Quota, RateLimiter};
use nonzero::nonzero;
use serde::{Deserialize, Serialize};

use deadpool_postgres::Pool;
use serde_json::{json, Value};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Config {
    pub pool: Pool,
    pub broadcaster: Arc<Broadcaster>,
    pub limits: Arc<Mutex<HashMap<String, AccountLimit>>>,
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
    account_limit: AccountLimit,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    if !account_limit.origins.is_empty() {
        match request.headers().get("host") {
            None => return Err(Error::User("missing host header".to_string())),
            Some(host) => {
                if let Ok(host) = host.to_str() {
                    if !account_limit
                        .origins
                        .contains(host.split(':').next().unwrap_or(host))
                    {
                        return Err(Error::User(format!("host {} not allowed", host)));
                    }
                }
            }
        }
    }
    if account_limit
        .rate
        .check_key(&conn_info.ip().to_string())
        .is_err()
    {
        return Err(Error::TooManyRequests(None));
    }
    match tokio::time::timeout(account_limit.timeout, next.run(request)).await {
        Ok(response) => Ok(response),
        Err(_) => Err(Error::Timeout(None)),
    }
}

pub async fn handle_service_error(error: tower::BoxError) -> Error {
    if error.is::<tower::load_shed::error::Overloaded>() {
        Error::Server(eyre!("server is overloaded").into())
    } else {
        Error::Server(eyre!("unknown").into())
    }
}

#[derive(Clone, Debug)]
pub struct AccountLimit {
    secret: String,
    origins: HashSet<String>,
    timeout: Duration,
    rate: Arc<governor::DefaultKeyedRateLimiter<String>>,
}

impl AccountLimit {
    fn default() -> Self {
        AccountLimit {
            secret: String::default(),
            origins: HashSet::new(),
            timeout: Duration::from_secs(10),
            rate: Arc::new(governor::DefaultKeyedRateLimiter::dashmap(
                Quota::per_second(nonzero!(10u32)),
            )),
        }
    }
    #[tracing::instrument(skip_all)]
    pub async fn from_pg(
        chain_id: i64,
        pg: &tokio_postgres::Client,
    ) -> eyre::Result<HashMap<String, AccountLimit>> {
        Ok(pg
            .query(
                "
                    select secret, timeout, rate, origins
                    from account_limits
                    where $1 = any(chains)
                ",
                &[&chain_id],
            )
            .await?
            .iter()
            .map(|row| AccountLimit {
                secret: row.get("secret"),
                timeout: Duration::from_secs(row.get::<&str, i32>("timeout") as u64),
                rate: Arc::new(RateLimiter::keyed(Quota::per_second(
                    NonZeroU32::new(row.get::<&str, i32>("rate") as u32).unwrap(),
                ))),
                origins: row
                    .get::<&str, Vec<String>>("origins")
                    .into_iter()
                    .collect(),
            })
            .map(|al| (al.secret.clone(), al))
            .collect())
    }
}

#[axum::async_trait]
impl FromRequestParts<Config> for AccountLimit {
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        config: &Config,
    ) -> Result<Self, Self::Rejection> {
        let params = parts.uri.query().unwrap_or_default();
        let decoded =
            serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
        let client_id = decoded.get("api-key").cloned().unwrap_or_default();
        tracing::Span::current().record("api-key", &client_id.clone()[..4]);
        Ok(config
            .limits
            .lock()
            .unwrap()
            .entry(client_id.clone())
            .or_insert_with(AccountLimit::default)
            .clone())
    }
}
