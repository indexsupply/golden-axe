use alloy::primitives::U256;
use eyre::{eyre, Result};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};

use crate::indexsupply::{self, Row};

#[derive(Clone)]
pub struct Client {
    daimo_key: Option<String>,
    address: String,
    reqwest: reqwest::Client,
    is: indexsupply::Client,
}

#[derive(Serialize)]
struct Item {
    name: String,
    description: String,
}

#[derive(Serialize)]
struct Recipient {
    address: String,
    amount: String,
    token: String,
    chain: u64,
}

#[derive(Serialize)]
struct Request {
    intent: String,
    items: Vec<Item>,
    recipient: Recipient,
    #[serde(rename = "redirectUri")]
    redirect_uri: String,
}

#[derive(Deserialize)]
pub struct PaymentLink {
    pub id: String,
    pub url: String,
}

static INTENT_FINISHED : &str = "IntentFinished(address indexed intentAddr, address indexed destinationAddr, bool indexed success,(uint256 toChainId, (address token, uint256 amount)[] bridgeTokenOutOptions, (address token, uint256 amount) finalCallToken, (address to, uint256 value, bytes data) finalCall, address bridger, address escrow, address refundAddress, uint256 nonce, uint256 expiration) intent)";

impl Client {
    pub fn new(daimo_key: Option<String>, is_key: Option<String>, be_url: String) -> Client {
        Client {
            address: String::from("0x7531f00DbC616b3466990e615bf01EfF507c88D4"),
            daimo_key,
            reqwest: reqwest::Client::new(),
            is: indexsupply::Client::new(is_key, be_url),
        }
    }

    ///amount is number of 1/100th dollars (penny)
    pub async fn generate(
        &self,
        name: &str,
        amount: i64,
        redirect_uri: &str,
    ) -> Result<PaymentLink, shared::Error> {
        let resp = self
            .reqwest
            .post("https://pay.daimo.com/api/generate")
            .header("Api-Key", self.daimo_key.as_ref().unwrap())
            .header("Idempotency-Key", random_ikey())
            .json(&Request {
                intent: String::from("Index Supply, Co."),
                items: vec![Item {
                    name: capitalize(name).to_string(),
                    description: String::from("1 year subscription"),
                }],
                recipient: Recipient {
                    address: self.address.clone(),
                    amount: (amount * 10000).to_string(),
                    token: String::from("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                    chain: 8453,
                },
                redirect_uri: redirect_uri.to_string(),
            })
            .send()
            .await?;
        if !resp.status().is_success() {
            let error_message = resp.text().await?;
            Err(shared::Error::Server(eyre!(error_message).into()))
        } else {
            let link = resp.json::<PaymentLink>().await?;
            tracing::info!("new daimo payment link nonce: {}", nonce(&link.id));
            Ok(link)
        }
    }

    pub async fn check(&self, daimo_id: &str) -> Result<Option<String>, shared::Error> {
        tracing::info!("checking for daimo payment: {}", nonce(daimo_id));
        let res: Vec<Row> = self
            .is
            .query(
                8453,
                vec![INTENT_FINISHED],
                &format!(
                    r#"
                    select tx_hash
                    from intentfinished
                    where destinationAddr = 0x7531f00DbC616b3466990e615bf01EfF507c88D4
                    and success = true
                    and intent->>'nonce' = '{}'
                    "#,
                    nonce(daimo_id)
                ),
            )
            .await?;
        if res.len() >= 2 {
            let tx_hash = res[1][0].as_str().unwrap_or_default().to_string();
            tracing::info!(
                "completed daimo payment nonce: {} tx: {}",
                nonce(daimo_id),
                tx_hash
            );
            Ok(Some(tx_hash))
        } else {
            Ok(None)
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Client::new(None, None, String::from("http://localhost:8000"))
    }
}

pub fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub fn nonce(id: &str) -> U256 {
    let decoded = bs58::decode(id).into_vec().unwrap_or_else(|_| vec![]);
    U256::try_from_be_slice(decoded.as_slice()).unwrap_or_default()
}

fn random_ikey() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
