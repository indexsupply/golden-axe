use alloy::primitives::{Address, BlockHash, Bytes, FixedBytes, U256, U64};
use serde::Deserialize;
use std::{fmt, time::Duration};

#[derive(Clone, Deserialize, Debug)]
pub struct Error {
    pub code: i64,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "jrpc: {} {}", self.code, self.message)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error {
            code: err.status().unwrap_or_default().as_u16() as i64,
            message: err.to_string(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Log {
    #[serde(rename = "blockNumber")]
    pub block_number: U64,
    #[serde(rename = "transactionHash")]
    pub tx_hash: FixedBytes<32>,
    #[serde(rename = "logIndex")]
    pub log_idx: U64,

    pub address: FixedBytes<20>,
    pub topics: Vec<FixedBytes<32>>,
    pub data: Bytes,
}

#[derive(Deserialize, Debug)]
pub struct Tx {
    pub hash: BlockHash,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
}

#[derive(Deserialize, Debug)]
pub struct Block {
    pub hash: BlockHash,
    #[serde(rename = "parentHash")]
    pub parent_hash: BlockHash,
    pub number: U64,
    pub transactions: Vec<Tx>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum EthItem {
    Uint(U64),
    Block(Block),
    Tx(Tx),
    Log(Vec<Log>),
}

macro_rules! impl_try_from_response {
    ($type:ty, $variant:path) => {
        impl TryFrom<Response> for $type {
            type Error = Error;

            fn try_from(response: Response) -> Result<Self, Self::Error> {
                match response.result {
                    None => Err(Error {
                        code: 0,
                        message: String::from("no result"),
                    }),
                    Some($variant(value)) => Ok(value),
                    _ => Err(Error {
                        code: 0,
                        message: String::from("incompatible type"),
                    }),
                }
            }
        }
    };
}

impl_try_from_response!(Block, EthItem::Block);
impl_try_from_response!(Tx, EthItem::Tx);
impl_try_from_response!(Vec<Log>, EthItem::Log);
impl_try_from_response!(U64, EthItem::Uint);

#[derive(Debug, Deserialize)]
pub struct Response {
    result: Option<EthItem>,
    error: Option<Error>,
}

impl Response {
    pub fn to<T: TryFrom<Response, Error = Error>>(self) -> Result<T, Error> {
        T::try_from(self)
    }
}

#[derive(Default)]
pub struct Client {
    url: String,
    http_client: reqwest::Client,
}

impl Client {
    pub fn new(url: &str) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .gzip(true)
            .deflate(true)
            .build()
            .unwrap();
        Client {
            http_client,
            url: url.to_string(),
        }
    }

    pub async fn send_one(&self, request: serde_json::Value) -> Result<Response, Error> {
        let decoded = self
            .http_client
            .post(&self.url)
            .json(&request)
            .send()
            .await?
            .json::<Response>()
            .await?;
        if let Some(e) = decoded.error {
            Err(e)
        } else {
            Ok(decoded)
        }
    }

    pub async fn send(&self, request: serde_json::Value) -> Result<Vec<Response>, Error> {
        let response = self
            .http_client
            .post(&self.url)
            .json(&request)
            .send()
            .await?;
        let decoded = response.json::<Vec<Response>>().await?;
        match decoded.iter().find_map(|r| r.error.clone()) {
            Some(e) => Err(e),
            None => Ok(decoded),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{b256, fixed_bytes, U64};

    use crate::jrpc::EthItem;

    #[test_log::test(tokio::test)]
    async fn test_batch() {
        let client = super::Client::new("https://eth.merkle.io/");
        let block_number = U64::from(12911679);
        client
            .send(serde_json::json!([
            {
                "id": "1",
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [block_number, true],
            },
            {
                "id": "1",
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [{"fromBlock": block_number, "toBlock": block_number}],
            }]))
            .await
            .unwrap()
            .iter()
            .for_each(|resp| match resp.result.as_ref() {
                Some(EthItem::Block(b)) => {
                    assert_eq!(
                        b.hash,
                        b256!("a917fcc721a5465a484e9be17cda0cc5493933dd3bc70c9adbee192cb419c9d7")
                    );
                    assert_eq!(
                        b.transactions.first().unwrap().hash,
                        b256!("23e3362a76c8b9370dc65bac8eb1cda1d408ac238a466cfe690248025254bf52")
                    )
                }
                Some(EthItem::Log(logs)) => {
                    let l = logs.first().expect("missing logs");
                    assert_eq!(l.block_number, block_number);
                    assert_eq!(
                        l.address,
                        fixed_bytes!("1f573d6fb3f13d689ff844b4ce37794d79a7ff1c")
                    );
                    assert_eq!(l.topics.len(), 3);
                    assert_eq!(l.data.len(), 32);
                }
                _ => panic!("unexpected results!"),
            });
    }
}
