use eyre::{eyre, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct Client {
    #[allow(dead_code)]
    key: Option<String>,
    http_client: reqwest::Client,
}

pub type Row = Vec<serde_json::Value>;

#[derive(Debug, Deserialize)]
struct Response {
    #[allow(dead_code)]
    block_height: u64,
    result: Vec<Vec<Row>>,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    event_signatures: Vec<&'a str>,
    query: String,
}

impl Client {
    pub fn new(key: Option<String>) -> Client {
        Client {
            key,
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn query(
        &self,
        chain: u64,
        signatures: Vec<&str>,
        query: &str,
    ) -> Result<Vec<Row>, shared::Error> {
        let resp = self
            .http_client
            .post("https://api.indexsupply.net/query")
            .query(&[
                ("chain", chain.to_string()),
                ("api-key", "8d622273f07ea179e6d50177ef6ca94d".to_string()),
            ])
            .json(&vec![Request {
                event_signatures: signatures,
                query: query.to_string(),
            }])
            .send()
            .await
            .wrap_err("making index supply request")?;
        if !resp.status().is_success() {
            let error_message = resp.text().await?;
            Err(shared::Error::Server(eyre!(error_message).into()))
        } else {
            let api_response = resp.json::<Response>().await?;
            Ok(api_response.result[0].clone())
        }
    }
}
