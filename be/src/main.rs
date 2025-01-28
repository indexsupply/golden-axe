use std::{future::IntoFuture, net::SocketAddr, time::Duration};

use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{connect_info::IntoMakeServiceWithConnectInfo, MatchedPath},
    routing::{get, post, Router},
};
use be::{api, api_sql, sync};
use clap::Parser;
use tower::ServiceBuilder;
use tower_http::{
    classify::ServerErrorsFailureClass, compression::CompressionLayer, cors::CorsLayer,
    trace::TraceLayer,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(
        long = "pg-be",
        env = "PG_URL",
        default_value = "postgres://localhost/be"
    )]
    pg_url: String,

    #[arg(
        long = "pg-ro",
        env = "PG_URL_RO",
        default_value = "postgres://uapi:XXX@localhost/be"
    )]
    pg_url_ro: String,

    #[arg(
        long = "pg-fe",
        env = "PG_URL_FE",
        default_value = "postgres://localhost/fe?application_name=be"
    )]
    pg_url_fe: String,

    #[arg(env = "MAX_PG_CONNS")]
    max_pg_conns: Option<usize>,

    #[arg(short = 'l', env = "LISTEN", default_value = "0.0.0.0:8000")]
    listen: String,

    #[clap(long, env = "NO_SYNC", action = clap::ArgAction::SetTrue)]
    no_sync: bool,
}

static SCHEMA_BE: &str = include_str!("./sql/schema.sql");

#[tokio::main]
async fn main() {
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
        shared::pg::new_pool(&args.pg_url, args.max_pg_conns.unwrap_or(32)).expect("pg_be pool"),
        shared::pg::new_pool(&args.pg_url_fe, args.max_pg_conns.unwrap_or(4)).expect("pg_fe pool"),
        shared::pg::new_pool(&args.pg_url_ro, args.max_pg_conns.unwrap_or(32)).expect("pg_ro pool"),
    );

    config
        .be_pool
        .get()
        .await
        .expect("backend pool")
        .batch_execute(SCHEMA_BE)
        .await
        .expect("updating backend schema");

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .expect("binding to tcp for http server");

    if args.no_sync {
        match tokio::try_join!(
            tokio::spawn(account_limits(config.clone())),
            tokio::spawn(axum::serve(listener, service(config.clone())).into_future()),
        ) {
            Ok(_) => tracing::error!("task died too soon"),
            Err(e) => panic!("task failed {}", e),
        }
    } else {
        match tokio::try_join!(
            tokio::spawn(account_limits(config.clone())),
            tokio::spawn(sync::run(config.clone())),
            tokio::spawn(axum::serve(listener, service(config.clone())).into_future()),
        ) {
            Ok(_) => tracing::error!("task died too soon"),
            Err(e) => panic!("task failed {}", e),
        }
    }
}

async fn account_limits(config: api::Config) {
    loop {
        if let Some(limits) = config.gafe.load_account_limits().await {
            *config.account_limits.lock().unwrap() = limits;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
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
                 ua = tracing::field::Empty,
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

    use super::service;
    use super::SCHEMA_BE;
    use be::{api, api_sql, sync};

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
            let partition_stmt = format!(
                r#"create table if not exists "logs_{}" partition of logs for values in ({})"#,
                $chain, $chain
            );
            pgtx.execute(&partition_stmt, &[])
                .await
                .expect("creating partition");
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
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        let config = api::Config::new(pool.clone(), pool.clone(), pool.clone());
        let server = TestServer::new(service(config)).unwrap();
        server.get("/").await.assert_text_contains("hello");
    }

    #[tokio::test]
    async fn test_query_post_with_params() {
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        sol! {
            #[sol(abi)]
            event Foo(uint a);
        };
        add_log!(pool, api::Chain(1), 1, Foo { a: U256::from(42) });

        let config = api::Config::new(pool.clone(), pool.clone(), pool.clone());
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
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        sol! {
            #[sol(abi)]
            event Foo(uint a);
        };

        let config = api::Config::new(pool.clone(), pool.clone(), pool.clone());
        let server = TestServer::new(service(config.clone())).unwrap();
        let request = api_sql::Request {
            api_key: None,
            chain: Some(api::Chain(1)),
            block_height: None,
            event_signatures: vec![Foo::abi().full_signature()],
            query: String::from("select a, block_num from foo"),
        };

        tokio::spawn(async move {
            let bcaster = config.api_updates.clone();
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

    #[tokio::test]
    async fn test_query_sse_error() {
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        sol! {
            #[sol(abi)]
            event Foo(uint a);
        };

        let config = api::Config::new(pool.clone(), pool.clone(), pool.clone());
        let server = TestServer::new(service(config.clone())).unwrap();
        let request = api_sql::Request {
            api_key: None,
            chain: Some(api::Chain(1)),
            block_height: None,
            event_signatures: vec![Foo::abi().full_signature()],
            query: String::from("select a, block_num from bar"),
        };

        tokio::spawn(async move {
            let bcaster = config.api_updates.clone();
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
            r#"You are attempting to query 'bar' but it isn't defined. Possible events to query are: 'logs, foo'""#
        );
    }
}
