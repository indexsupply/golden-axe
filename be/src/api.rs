use std::{
    collections::HashMap,
    convert::Infallible,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, FromRequest, FromRequestParts, Query, State},
    http::{request::Parts, StatusCode},
    response::{
        sse::{Event as SSEvent, KeepAlive},
        Sse,
    },
};
use axum_extra::{headers::UserAgent, TypedHeader};
use bytes::BufMut;
use eyre::eyre;
use futures::Stream;
use serde::{Deserialize, Serialize};

use deadpool_postgres::Pool;
use serde::ser::SerializeStruct;
use serde_json::{json, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::{broadcast, gafe};

macro_rules! user_error {
    ($e:expr) => {
        Err(Error::User(String::from($e)))
    };
}

pub async fn handle_service_error(error: tower::BoxError) -> Error {
    if error.is::<tower::load_shed::error::Overloaded>() {
        Error::Server(eyre!("server is overloaded").into())
    } else {
        Error::Server(eyre!("unknown").into())
    }
}

pub async fn handle_conns(
    State(config): State<Config>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    if params.get("secret").map(|s| s.as_str()) != Some(&config.admin_api_secret) {
        return Err(Error::User("no can do".into()));
    }
    let limits_copy = config.account_limits.lock().unwrap().clone();
    let conn_info: HashMap<String, usize> = limits_copy
        .iter()
        .map(|(secret, al)| {
            let conns = al.connections as usize - al.conn_limiter.available_permits();
            let mut key = secret.clone();
            key.truncate(4);
            (key, conns)
        })
        .filter(|(_, v)| *v > 0)
        .collect();
    let v = serde_json::to_value(conn_info).unwrap();
    Ok(axum::Json(v))
}

pub async fn handle_status(
    State(conf): State<Config>,
) -> axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>> {
    let mut rx = conf.broadcaster.json_updates.subscribe();
    let config = conf.clone();
    let stream = async_stream::stream! {
        loop {
            let update = rx.recv().await.expect("unable to receive new block update");
            yield Ok(SSEvent::default()
                .json_data(update)
                .expect("unable to serialize json"));
            let active_connections = MAX_ACTIVE_CONNECTIONS - config.active_connections.available_permits();
            yield Ok(SSEvent::default()
                .json_data(serde_json::json!({ "active_connections": active_connections }))
                .expect("unable to serialize json"));
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Clone)]
pub struct Config {
    pub admin_api_secret: String,
    pub be_pool: Pool,
    pub fe_pool: Pool,
    pub ro_pool: Pool,
    pub broadcaster: Arc<broadcast::Channel>,
    pub active_connections: Arc<Semaphore>,
    pub open_limit: Arc<gafe::AccountLimit>,
    pub free_limit: Arc<gafe::AccountLimit>,
    pub account_limits: Arc<Mutex<HashMap<String, Arc<gafe::AccountLimit>>>>,
    pub gafe: gafe::Connection,
}

const MAX_ACTIVE_CONNECTIONS: usize = 10000;

impl Config {
    pub fn new(admin_api_secret: String, be_pool: Pool, fe_pool: Pool, ro_pool: Pool) -> Config {
        Config {
            admin_api_secret,
            gafe: gafe::Connection::new(fe_pool.clone()),
            broadcaster: Arc::new(broadcast::Channel::default()),
            active_connections: Arc::new(Semaphore::new(MAX_ACTIVE_CONNECTIONS)),
            account_limits: Arc::new(Mutex::new(HashMap::new())),
            free_limit: Arc::new(gafe::AccountLimit::free()),
            open_limit: Arc::new(gafe::AccountLimit::open()),
            be_pool,
            fe_pool,
            ro_pool,
        }
    }

    pub async fn new_connection(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.active_connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::TooManyRequests(Some("too many connections".into())))
    }
}

#[derive(Debug)]
pub enum Error {
    User(String),
    Timeout(Option<String>),
    TooManyRequests(Option<String>),

    Server(Box<dyn std::error::Error + Send + Sync>),
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Error", 2)?;
        match self {
            Error::User(msg) => {
                state.serialize_field("error", "user")?;
                state.serialize_field("message", msg)?;
            }
            Error::Timeout(opt_msg) => {
                state.serialize_field("error", "timeout")?;
                state.serialize_field("message", &opt_msg)?;
            }
            Error::TooManyRequests(opt_msg) => {
                state.serialize_field("error", "too_many_requests")?;
                state.serialize_field("message", &opt_msg)?;
            }
            Error::Server(err) => {
                state.serialize_field("error", "server")?;
                state.serialize_field("message", &err.to_string())?;
            }
        }
        state.end()
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::User(msg) => write!(f, "User error: {}", msg),
            Error::Timeout(Some(msg)) => write!(f, "Operation timed out: {}", msg),
            Error::Timeout(None) => write!(f, "Operation timed out"),
            Error::TooManyRequests(Some(msg)) => write!(f, "Too many requests: {}", msg),
            Error::TooManyRequests(None) => write!(f, "Too many requests"),
            Error::Server(err) => write!(f, "Server error: {}", err),
        }
    }
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

impl From<deadpool_postgres::PoolError> for Error {
    fn from(err: deadpool_postgres::PoolError) -> Self {
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

pub async fn limit(
    origin_ip: OriginIp,
    origin_domain: Option<OriginDomain>,
    account_limit: Arc<gafe::AccountLimit>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    if !account_limit.origins.is_empty() {
        match origin_domain {
            None => tracing::error!("missing origin"),
            Some(domain) => {
                if !account_limit.origins.contains(domain.0.as_str()) {
                    tracing::error!("origin {:?} not allowed", domain);
                }
            }
        }
    }
    if account_limit
        .rate_limiter
        .check_key(&origin_ip.to_string())
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

#[derive(Clone, Copy, Default, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Chain(pub u64);

pub trait ChainOptionExt {
    fn unwrap_chain(self) -> Result<Chain, Error>;
}

impl ChainOptionExt for Option<Chain> {
    fn unwrap_chain(self) -> Result<Chain, Error> {
        self.ok_or_else(|| Error::User(String::from("missing chain")))
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Chain {
    fn from(value: u64) -> Self {
        Chain(value)
    }
}

impl tokio_postgres::types::ToSql for Chain {
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(*ty, tokio_postgres::types::Type::INT8)
    }
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send + 'static>>
    {
        if matches!(*ty, tokio_postgres::types::Type::INT8) {
            out.put_i64(self.0 as i64);
            Ok(tokio_postgres::types::IsNull::No)
        } else {
            Err(Box::new(tokio_postgres::types::WrongType::new::<Self>(
                ty.clone(),
            )))
        }
    }
    tokio_postgres::types::to_sql_checked!();
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Chain {
    type Rejection = Error;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let params = parts.uri.query().unwrap_or_default();
        let decoded =
            serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
        if let Some(chain) = decoded.get("chain").cloned().and_then(|c| c.parse().ok()) {
            Ok(Chain(chain))
        } else if let Some(chain) = parts
            .headers
            .get("chain")
            .and_then(|c| c.to_str().ok())
            .and_then(|c| c.parse().ok())
        {
            Ok(Chain(chain))
        } else {
            user_error!("must supply Chain header or chain query parameter")
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Key(String);

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Key {
    pub fn short(&self) -> String {
        self.0[..self.0.len().min(4)].to_string()
    }
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Key {
    type Rejection = Error;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let params = parts.uri.query().unwrap_or_default();
        let decoded =
            serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
        Ok(Key(decoded
            .get("api-key")
            .or_else(|| decoded.get("api_key"))
            .cloned()
            .unwrap_or_default()))
    }
}

#[derive(Debug)]
pub struct OriginDomain(String);

impl fmt::Display for OriginDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for OriginDomain {
    type Rejection = Error;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        if let Some(origin_header) = parts.headers.get("origin") {
            if let Ok(origin) = origin_header.to_str() {
                if let Ok(origin) = Url::parse(origin) {
                    if let Some(domain) = origin.domain() {
                        return Ok(OriginDomain(domain.to_string()));
                    }
                }
            }
        }
        user_error!("missing origin domain")
    }
}

#[derive(Clone)]
pub struct OriginIp(String);

impl fmt::Display for OriginIp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for OriginIp {
    type Rejection = Error;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let ip = parts
            .headers
            .get("X-Forwarded-For")
            .and_then(|origin_ip| origin_ip.to_str().ok())
            .map(String::from)
            .or_else(|| {
                parts
                    .extensions
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|ConnectInfo(addr)| addr.ip().to_string())
            })
            .ok_or_else(|| Error::User("unable to get ip address".to_string()))?;
        Ok(OriginIp(ip))
    }
}

#[axum::async_trait]
impl FromRequestParts<Config> for Arc<gafe::AccountLimit> {
    type Rejection = Error;
    async fn from_request_parts(
        parts: &mut Parts,
        config: &Config,
    ) -> Result<Self, Self::Rejection> {
        if !config.gafe.enabled() {
            return Ok(config.open_limit.clone());
        }
        let params = parts.uri.query().unwrap_or_default();
        let decoded =
            serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
        let client_id = decoded.get("api-key").cloned().unwrap_or_default();
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

pub async fn content_length_header(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    let response = next.run(request).await;
    let span = tracing::Span::current();
    response
        .headers()
        .get("content-length")
        .and_then(|cl| cl.to_str().ok())
        .map(|cl| cl.parse::<u64>().ok())
        .map(|size| span.record("size", size));
    Ok(response)
}

pub async fn log_fields(
    ua: Option<TypedHeader<UserAgent>>,
    ip: OriginIp,
    origin: Option<OriginDomain>,
    key: Option<Key>,
    chain: Option<Chain>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    let span = tracing::Span::current();
    span.record("ip", ip.to_string());
    ua.map(|v| {
        span.record(
            "ua",
            v.as_str().split_whitespace().next().unwrap_or(v.as_str()),
        )
    });
    origin.map(|v| span.record("origin", v.to_string()));
    key.map(|v| span.record("key", v.short()));
    chain.map(|v| span.record("chain", v.0));
    Ok(next.run(request).await)
}
