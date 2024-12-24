mod api;
mod api_sql;
mod gafe;
mod pg;
mod s256;
mod sql_generate;
mod sql_test;
mod sync;
mod user_query;

use std::{
    future::{ready, IntoFuture},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{connect_info::IntoMakeServiceWithConnectInfo, MatchedPath},
    routing::{get, post, Router},
};
use clap::Parser;
use eyre::{eyre, Result};
use futures::TryFutureExt;
use itertools::Itertools;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use sync::{Config, Downloader};
use tower::ServiceBuilder;
use tower_http::{
    classify::ServerErrorsFailureClass, compression::CompressionLayer, cors::CorsLayer,
    trace::TraceLayer,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(long, env = "START_BLOCK")]
    start_block: Option<u64>,

    #[arg(long = "pg", env = "PG_URL", default_value = "postgres://localhost/ga")]
    pg_url: String,

    #[arg(
        long = "pg-ro",
        env = "PG_URL_RO",
        default_value = "postgres://uapi:XXX@localhost/ga"
    )]
    pg_url_ro: String,

    #[arg(long = "pg-gafe", env = "PG_URL_GAFE")]
    pg_url_gafe: Option<String>,

    #[arg(short = 'l', env = "LISTEN", default_value = "0.0.0.0:8000")]
    listen: String,

    #[clap(long, action = clap::ArgAction::SetTrue)]
    no_sync: bool,
}

static SCHEMA: &str = include_str!("./sql/schema.sql");

#[tokio::main]
async fn main() -> Result<(), api::Error> {
    let fmt_layer = fmt::layer()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .compact();
    let filter_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(MetricsLayer::new())
        .with(fmt_layer)
        .with(filter_layer)
        .init();

    let args = Args::parse();
    let config = api::Config::new(
        pg::new_pool(&args.pg_url, 32)?,
        args.pg_url_gafe
            .as_ref()
            .and_then(|url| pg::new_pool(url, 4).ok()),
    );

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .expect("binding to tcp for http server");

    let res = tokio::try_join!(
        flatten(tokio::spawn(account_limits(config.clone()))),
        flatten(tokio::spawn(sync(
            args.clone(),
            config.remote_broadcaster.clone(),
            config.broadcaster.clone(),
        ))),
        flatten(tokio::spawn(
            axum::serve(listener, service(config.clone()))
                .into_future()
                .map_err(|e| eyre!("serving http: {}", e))
        )),
    );
    match res {
        Err(err) => panic!("{}", err),
        _ => Ok(()),
    }
}

async fn sync(
    args: Args,
    remote_broadcaster: Arc<api::Broadcaster2>,
    broadcaster: Arc<api::Broadcaster>,
) -> Result<()> {
    if args.no_sync {
        tracing::info!("no sync");
        return Ok(());
    }
    let pg_pool = pg::new_pool(&args.pg_url, 16)?;
    let pg = pg_pool.get().await?;
    pg.batch_execute(SCHEMA).await.unwrap();
    let tasks = Config::load(&pg)
        .await?
        .into_iter()
        .filter(|c| c.enabled)
        .map(|config| {
            let ch = broadcaster.clone();
            let rb = remote_broadcaster.clone();
            let pg = pg_pool.clone();
            tokio::spawn(async move {
                Downloader::new(pg, rb, config, args.start_block)
                    .run(ch)
                    .await
            })
        })
        .collect_vec();
    for t in tasks {
        if let Err(e) = t.await {
            return Err(e.into());
        }
    }
    Ok(())
}

async fn account_limits(config: api::Config) -> Result<()> {
    loop {
        if let Some(limits) = config.gafe.load_account_limits().await {
            *config.account_limits.lock().unwrap() = limits;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(eyre!("handle error: {}", err)),
    }
}

fn service(config: api::Config) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr> {
    let prom_record = PrometheusBuilder::new()
        .add_global_label("name", "ga")
        .build_recorder();
    let prom_handler = prom_record.handle();
    metrics::set_global_recorder(TracingContextLayer::all().layer(prom_record))
        .expect("unable to set global metrics recorder");
    let tracing = TraceLayer::new_for_http()
        .make_span_with(|req: &axum::http::Request<Body>| {
             let path = req
                 .extensions()
                 .get::<MatchedPath>()
                 .map(MatchedPath::as_str);
             tracing::info_span!("http",
                 "api-key" = tracing::field::Empty,
                 "ip" = tracing::field::Empty,
                 "size" = tracing::field::Empty,
                 status = tracing::field::Empty,
                 path,
                 chain = tracing::field::Empty,
             )
        })
        .on_response(
            |resp: &axum::http::Response<_>, d: Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_u16());
                let _guard = span.enter();
                metrics::counter!("api.requests").increment(1);
                metrics::histogram!("api.latency").record(d.as_millis() as f64);
                if !resp.status().is_success() {
                    metrics::counter!("api.errors").increment(1);
                }
            },
        )
        .on_failure(
            |error: ServerErrorsFailureClass, _latency: Duration, _span: &tracing::Span| {
                tracing::error!(error = %error)
            },
        );
    let service = ServiceBuilder::new()
        .layer(axum::middleware::from_fn(api::latency_header))
        .layer(tracing)
        .layer(axum::middleware::from_fn(api::content_length_header))
        .layer(HandleErrorLayer::new(api::handle_service_error))
        .load_shed()
        .concurrency_limit(1024)
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn_with_state(
            config.clone(),
            api::limit,
        ))
        .layer(CompressionLayer::new());
    Router::new()
        .route("/", get(|| async { "hello\n" }))
        .route("/metrics", get(move || ready(prom_handler.render())))
        .route("/status", get(api::handle_status))
        .route("/query", get(api_sql::handle_get))
        .route("/query", post(api_sql::handle_post))
        .route("/query-live", get(api_sql::handle_sse))
        .layer(service)
        .with_state(config.clone())
        .into_make_service_with_connect_info::<SocketAddr>()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, B256, U64};
    use axum_test::TestServer;
    use serde_json::json;

    use super::*;
    use crate::pg;

    #[tokio::test]
    async fn test_index() {
        let (_pg_server, pool) = pg::test_utils::test_pg().await;
        let config = api::Config::new(pool, None);
        let server = TestServer::new(service(config)).unwrap();
        server.get("/").await.assert_text_contains("hello");
    }

    #[tokio::test]
    async fn test_query_post_with_params() {
        let (_pg_server, pool) = pg::test_utils::test_pg().await;
        let pg = pool.get().await.expect("unable to get pg from pool");
        pg::test_utils::insert(
            &pg,
            vec![pg::test_utils::Log {
                chain: U64::from(1),
                block_num: U64::from(1),
                topics: vec![B256::with_last_byte(0x42)],
                data: Bytes::from_static(&[0x42]),
            }],
        )
        .await;
        let config = api::Config::new(pool, None);
        let server = TestServer::new(service(config)).unwrap();
        let request = vec![api_sql::Request {
            block_height: None,
            event_signatures: vec![],
            query: String::from("select block_num from logs"),
        }];
        server
            .post("/query")
            .add_query_param("api-key", "face")
            .add_query_param("chain", "1")
            .json(&request)
            .await
            .assert_json(&json!({
                "block_height": 1,
                "result": [[["block_num"],[1]]]
            }));
    }
}
