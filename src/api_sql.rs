use std::{convert::Infallible, sync::Arc};

use alloy::{
    hex,
    primitives::{Bytes, U64},
};
use axum::{
    extract::State,
    response::{
        sse::{Event as SSEvent, KeepAlive},
        Sse,
    },
    Json,
};
use axum_extra::extract::Form;
use eyre::{Context, Result};
use futures::Stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;

use crate::{api, s256, sql_generate};

pub async fn handle(
    State(state): State<Arc<api::Config>>,
    Form(req): Form<Request>,
) -> Result<Json<Response>, api::Error> {
    handle_json(State(state.clone()), api::Json(vec![req.clone()])).await
}

pub async fn handle_sse(
    State(conf): State<Arc<api::Config>>,
    Form(req): Form<Request>,
) -> axum::response::Sse<impl Stream<Item = Result<SSEvent, Infallible>>> {
    let mut req = req.clone();
    let mut rx = conf.broadcaster.add();
    let stream = async_stream::stream! {
        loop {
            let resp = handle_json(State(conf.clone()), api::Json(vec![req.clone()])).await.expect("unable to make request");
            yield Ok(SSEvent::default().json_data(resp.0).expect("unable to seralize json"));
            req.block_height = Some(rx.recv().await.expect("unable to receive new block update"));
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Request {
    pub event_signatures: Vec<String>,
    pub query: String,
    pub block_height: Option<u64>,
}

type Row = Vec<Value>;
type Rows = Vec<Row>;

#[derive(Deserialize, Serialize)]
pub struct Response {
    pub block_height: u64,
    pub result: Vec<Rows>,
}

pub async fn handle_json(
    State(state): State<Arc<api::Config>>,
    api::Json(req): api::Json<Vec<Request>>,
) -> Result<Json<Response>, api::Error> {
    let mut pg = state.pool.get().await.wrap_err("getting conn from pool")?;
    let pgtx = pg
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .wrap_err("starting sql api read tx")?;
    let mut res: Vec<Rows> = Vec::new();
    for r in req {
        let query = sql_generate::query(
            &r.query,
            r.event_signatures.iter().map(|s| s.as_str()).collect(),
            r.block_height,
        )?;
        tracing::info!("query: {}", query);
        res.push(handle_rows(pgtx.query(&query, &[]).await?)?);
    }
    Ok(Json(Response {
        block_height: pgtx
            .query_one("select max(num)::text from blocks", &[])
            .await?
            .get::<usize, U64>(0)
            .to::<u64>(),
        result: res,
    }))
}

fn handle_rows(rows: Vec<tokio_postgres::Row>) -> Result<Rows, api::Error> {
    let mut result: Rows = Vec::new();
    if let Some(first) = rows.first() {
        result.push(
            first
                .columns()
                .iter()
                .map(|c| Value::String(c.name().to_string()))
                .collect(),
        );
    }
    for row in rows {
        let mut json_row: Vec<Value> = Vec::new();
        for (idx, column) in row.columns().iter().enumerate() {
            let value = match *column.type_() {
                Type::BOOL => {
                    let b: bool = row.get(idx);
                    Value::Bool(b)
                }
                Type::NUMERIC => {
                    let n: s256::Int = row.get(idx);
                    Value::String(n.to_string())
                }
                Type::INT2 => {
                    let n: i16 = row.get(idx);
                    Value::Number(n.into())
                }
                Type::INT4 => {
                    let n: i32 = row.get(idx);
                    Value::Number(n.into())
                }
                Type::INT8 => {
                    let n: i64 = row.get(idx);
                    Value::Number(n.into())
                }
                Type::BYTEA => {
                    let b: &[u8] = row.get(idx);
                    Value::String(hex::encode_prefixed(b))
                }
                Type::TEXT => {
                    let s: String = row.get(idx);
                    Value::String(s)
                }
                Type::NUMERIC_ARRAY => {
                    let nums: Vec<s256::Int> = row.get(idx);
                    serde_json::json!(nums.iter().map(|n| n.to_string()).collect::<Vec<String>>())
                }
                Type::BYTEA_ARRAY => {
                    let arrays: Vec<Vec<u8>> = row.get::<usize, Vec<Vec<u8>>>(idx);
                    serde_json::json!(arrays
                        .iter()
                        .map(|array| Bytes::copy_from_slice(array))
                        .collect_vec())
                }
                _ => Value::Null,
            };
            json_row.push(value);
        }
        result.push(json_row)
    }
    Ok(result)
}

pub mod cli {
    use crate::{
        api::{self, client_post},
        sql_generate,
    };
    use alloy::{json_abi::Event, primitives::Address};
    use clap::Args;
    use eyre::{Context, Result};
    use itertools::Itertools;
    use reqwest::Client;
    use serde_json::Value;
    use std::{
        fs::File,
        io::{self, BufRead, Write},
        path::Path,
    };
    use url::Url;

    pub const HELP: &str = include_str!("./cli-help/query.txt");

    pub fn print_view(args: &Request) -> Result<()> {
        if let Some(events) = parse_events(&args.events_file, &args.event)? {
            for event in events {
                println!("{}", sql_generate::fmt_sql(&sql_generate::view(&event)?)?);
            }
        }
        Ok(())
    }

    pub fn parse_events(
        events_file: &Option<String>,
        event: &Option<String>,
    ) -> Result<Option<Vec<Event>>> {
        if let Some(path) = events_file {
            let path = Path::new(&path);
            let file = File::open(path)?;
            let reader = io::BufReader::new(file);
            let mut events: Vec<Event> = Vec::new();
            for line in reader.lines() {
                let data = line?;
                events
                    .push(Event::parse(&data).wrap_err(format!("unable to abi parse: {}", data))?);
            }
            Ok(Some(events))
        } else if let Some(event) = event {
            Ok(Some(vec![event
                .parse()
                .wrap_err(format!("unable to abi parse: {}", &event))?]))
        } else {
            Ok(None)
        }
    }

    #[derive(Args, Debug)]
    pub struct Request {
        #[arg(from_global)]
        url: Url,

        pub query: String,

        #[arg(from_global)]
        pub address: Option<Vec<Address>>,

        #[arg(short = 'b', help = "print block height at query")]
        pub block_height: bool,

        #[arg(from_global)]
        events_file: Option<String>,

        #[arg(from_global)]
        event: Option<String>,
    }

    pub async fn request(http_client: &Client, args: Request) -> Result<(), api::Error> {
        let event_signatures = parse_events(&args.events_file, &args.event)?
            .ok_or_else(|| api::Error::User("no events found".to_string()))?
            .iter()
            .map(|event| event.full_signature())
            .collect::<Vec<String>>();
        let req_body = super::Request {
            event_signatures,
            query: args.query,
            block_height: None,
        };

        let mut req_path = args.url.clone();
        req_path.set_path("/query");
        let res = client_post::<super::Response, _>(http_client, req_path, &vec![req_body]).await?;
        let rows = res.result.first().expect("no rows returned");

        if args.block_height {
            println!("block height: {}", res.block_height)
        }
        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        let out = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|r| match r {
                        Value::Array(_) => r
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|item| item.as_str().unwrap_or_default().to_string())
                            .join(","),
                        Value::Bool(b) => format!("{}", b),
                        _ => r.as_str().unwrap_or_default().to_string(),
                    })
                    .collect::<Vec<String>>()
                    .join("\t")
            })
            .join("\n");
        writeln!(tw, "{}", out).expect("unable to write to stdout");
        tw.flush().expect("unable to write to stdout");
        Ok(())
    }
}
