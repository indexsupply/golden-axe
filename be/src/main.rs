use std::{future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{connect_info::IntoMakeServiceWithConnectInfo, MatchedPath},
    routing::{get, post, Router},
};
use be::{
    api, api_sql, pg,
    sync::{Config, Downloader},
};
use clap::Parser;
use eyre::{eyre, Result};
use futures::TryFutureExt;
use itertools::Itertools;
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

    #[arg(long = "pg", env = "PG_URL", default_value = "postgres://localhost/be")]
    pg_url: String,

    #[arg(
        long = "pg-ro",
        env = "PG_URL_RO",
        default_value = "postgres://uapi:XXX@localhost/be"
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
    let tracing = TraceLayer::new_for_http()
        .make_span_with(|req: &axum::http::Request<Body>| {
            let method = req.method().as_str().to_lowercase();
            let path = req
                .extensions()
                 .get::<MatchedPath>()
                 .map(MatchedPath::as_str);
             tracing::info_span!("http",
                 method,
                 path,
                 ip = tracing::field::Empty,
                 origin = tracing::field::Empty,
                 key = tracing::field::Empty,
                 size = tracing::field::Empty,
                 status = tracing::field::Empty,
                 chain = tracing::field::Empty,
             )
        })
        .on_response(
            |resp: &axum::http::Response<_>, _: Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_u16());
            },
        )
        .on_failure(
            |error: ServerErrorsFailureClass, _: Duration, _span: &tracing::Span| {
                tracing::error!(error = %error)
            },
        );
    let service = ServiceBuilder::new()
        .layer(axum::middleware::from_fn(api::latency_header))
        .layer(tracing)
        .layer(axum::middleware::from_fn(api::log_fields))
        .layer(axum::middleware::from_fn(api::content_length_header))
        .layer(HandleErrorLayer::new(api::handle_service_error))
        .load_shed()
        .concurrency_limit(1024)
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn_with_state(
            config.clone(),
            api_sql::log_request,
        ))
        .layer(axum::middleware::from_fn_with_state(
            config.clone(),
            api::limit,
        ))
        .layer(CompressionLayer::new());
    Router::new()
        .route("/", get(|| async { "hello\n" }))
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
    use alloy::{
        primitives::{B256, U256, U64},
        sol,
        sol_types::{JsonAbiExt, SolEvent},
    };
    use axum_test::TestServer;
    use serde_json::json;

    use crate::SCHEMA;

    use super::service;
    use be::{api, api_sql, sync};
    use pg::test;

    macro_rules! add_log {
        ($pool:expr, $chain:expr, $block_num:expr, $event:expr) => {{
            let log = alloy::rpc::types::Log {
                inner: alloy::primitives::Log {
                    data: $event.encode_log_data(),
                    address: alloy::primitives::Address::with_last_byte(0xab),
                },
                block_number: Some($block_num),
                block_hash: Some(B256::with_last_byte(0xab)),
                block_timestamp: Some(1),
                transaction_hash: Some(B256::with_last_byte(0xab)),
                transaction_index: Some(1),
                log_index: Some(1),
                removed: false,
            };
            let mut pg = $pool
                .get()
                .await
                .expect("unable to get test pg client from pool");
            let pgtx = pg
                .transaction()
                .await
                .expect("unable to start new pgtx from pg pool");
            sync::copy(&pgtx, $chain, vec![log.clone()])
                .await
                .expect("unable to copy new logs");
            pgtx.execute(
                "insert into blocks(chain, num, hash) values ($1, $2, $3)",
                &[
                    &$chain,
                    &U64::from(log.block_number.unwrap()),
                    &log.block_hash.unwrap(),
                ],
            )
            .await
            .expect("unable to update blocks table");
            pgtx.commit()
                .await
                .expect("unable to commit the add_logs pg tx");
        }};
    }

    #[tokio::test]
    async fn test_index() {
        let (_pg_server, pool) = test::pg(SCHEMA).await;
        let config = api::Config::new(pool, None);
        let server = TestServer::new(service(config)).unwrap();
        server.get("/").await.assert_text_contains("hello");
    }

    #[tokio::test]
    async fn test_query_post_with_params() {
        let (_pg_server, pool) = test::pg(SCHEMA).await;
        sol! {
            #[sol(abi)]
            event Foo(uint a);
        };
        add_log!(pool, api::Chain(1), 1, Foo { a: U256::from(42) });

        let config = api::Config::new(pool, None);
        let server = TestServer::new(service(config)).unwrap();
        let request = vec![api_sql::Request {
            api_key: None,
            chain: None,
            block_height: None,
            event_signatures: vec![Foo::abi().full_signature()],
            query: String::from("select a, block_num from foo"),
        }];
        server
            .post("/query")
            .add_query_param("api-key", "face")
            .add_query_param("chain", "1")
            .json(&request)
            .await
            .assert_json(&json!({
                "block_height": 1,
                "result": [[["a", "block_num"],["42", 1]]]
            }));
    }

    #[tokio::test]
    async fn test_query_sse() {
        let (_pg_server, pool) = test::pg(SCHEMA).await;
        sol! {
            #[sol(abi)]
            event Foo(uint a);
        };

        let config = api::Config::new(pool.clone(), None);
        let server = TestServer::new(service(config.clone())).unwrap();
        let request = api_sql::Request {
            api_key: None,
            chain: Some(api::Chain(1)),
            block_height: None,
            event_signatures: vec![Foo::abi().full_signature()],
            query: String::from("select a, block_num from foo"),
        };

        tokio::spawn(async move {
            let bcaster = config.broadcaster.clone();
            for i in 1..=3 {
                add_log!(pool, api::Chain(1), i, Foo { a: U256::from(42) });
                bcaster.broadcast(api::Chain(1), i);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            bcaster.close(api::Chain(1));
        });
        let resp = server
            .get("/query-live")
            .add_raw_query_param(&serde_html_form::to_string(&request).unwrap())
            .await;
        resp.assert_text_contains(
            r#"data: {"block_height":1,"result":[[["a","block_num"],["42",1]]]}"#,
        );
        resp.assert_text_contains(
            r#"data: {"block_height":2,"result":[[["a","block_num"],["42",2]]]}"#,
        );
        resp.assert_text_contains(
            r#"data: {"block_height":3,"result":[[["a","block_num"],["42",3]]]}"#,
        );
    }
}
