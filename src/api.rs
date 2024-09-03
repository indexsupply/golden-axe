use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, FromRequest},
    http::StatusCode,
};
use eyre::eyre;
use serde::{Deserialize, Serialize};

use deadpool_postgres::Pool;
use serde_json::{json, Value};
use tokio::sync::{broadcast, Semaphore};

#[derive(Clone)]
pub struct Config {
    pub pool: Pool,
    pub broadcaster: Arc<Broadcaster>,
    pub limits: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
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

pub async fn rate_limit(
    axum::extract::State(config): axum::extract::State<Config>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, Error> {
    let params = request.uri().query().unwrap_or_default();
    let decoded = serde_urlencoded::from_str::<HashMap<String, String>>(params).unwrap_or_default();
    let client_id = decoded
        .get("api_key")
        .cloned()
        .unwrap_or_else(|| addr.ip().to_string());
    let semaphore = {
        let mut limiters = config.limits.lock().unwrap();
        limiters
            .entry(client_id.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(5)))
            .clone()
    };
    let req = async {
        let _permit = semaphore.acquire().await.unwrap();
        next.run(request).await
    };
    match tokio::time::timeout(Duration::from_secs(10), req).await {
        Ok(response) => Ok(response),
        Err(_) => Err(Error::User("request timed out".to_string())),
    }
}

pub async fn handle_service_error(error: tower::BoxError) -> Error {
    if error.is::<tower::load_shed::error::Overloaded>() {
        Error::Server(eyre!("server is overloaded").into())
    } else {
        Error::Server(eyre!("unknown").into())
    }
}
