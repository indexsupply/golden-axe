use alloy::primitives::{Address, BlockHash, Bytes, FixedBytes, U256, U64};
use itertools::Itertools;
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
    #[serde(rename = "blockTimestamp")]
    pub block_timestamp: Option<U64>,
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
    #[serde(rename = "type")]
    pub ty: Option<U64>,
    pub hash: BlockHash,
    #[serde(rename = "blockTimestamp")]
    pub block_timestamp: Option<U64>,
    #[serde(rename = "transactionIndex")]
    pub idx: U64,
    #[serde(default)]
    pub nonce: U256,
    pub from: Address,
    pub to: Option<Address>,
    #[serde(default)]
    pub input: Bytes,
    #[serde(default)]
    pub value: U256,
    pub gas: U256,
    #[serde(rename = "gasPrice")]
    pub gas_price: Option<U256>,
}

#[derive(Deserialize, Debug)]
pub struct Block {
    pub hash: BlockHash,
    #[serde(rename = "parentHash")]
    pub parent_hash: BlockHash,
    pub number: U64,
    pub nonce: U256,
    pub timestamp: U64,
    pub size: U64,
    pub transactions: Vec<Tx>,
    #[serde(rename = "gasLimit")]
    pub gas_limit: U256,
    #[serde(rename = "gasUsed")]
    pub gas_used: U256,
    #[serde(rename = "logsBloom")]
    pub logs_bloom: FixedBytes<256>,
    #[serde(rename = "receiptsRoot")]
    pub receipts_root: FixedBytes<32>,
    #[serde(rename = "stateRoot")]
    pub state_root: FixedBytes<32>,
    #[serde(rename = "extraData")]
    pub extra_data: Bytes,
    pub miner: Address,
}

#[derive(Default)]
pub struct Client {
    url: String,
    http_client: reqwest::Client,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RpcEither<T> {
    Ok { result: T },
    Err { error: Error },
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

    pub async fn chain_id(&self) -> Result<U64, Error> {
        let request = serde_json::json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
        });

        let response: RpcEither<U64> = self
            .http_client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error {
                code: -1,
                message: e.to_string(),
            })?
            .json()
            .await
            .map_err(|e| Error {
                code: -1,
                message: e.to_string(),
            })?;

        match response {
            RpcEither::Ok { result, .. } => Ok(result),
            RpcEither::Err { error, .. } => Err(error),
        }
    }

    #[tracing::instrument(level="info" skip_all, fields(from, to))]
    pub async fn blocks(&self, from: u64, to: u64) -> Result<Vec<Block>, Error> {
        let request: Vec<_> = (from..=to)
            .map(|n| {
                serde_json::json!({
                    "id": n,
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [format!("0x{:x}", n), true],
                })
            })
            .collect();
        let response_body = self
            .http_client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error {
                code: -1,
                message: format!("decoding blocks: {e:?}"),
            })?
            .text()
            .await
            .map_err(|e| Error {
                code: -1,
                message: format!("decoding blocks json: {e:?}"),
            })?;

        let response: Vec<RpcEither<Block>> =
            serde_json::from_str(&response_body).map_err(|e| Error {
                code: -1,
                message: format!("decode error: {e:?}\n{}\n", response_body),
            })?;

        Ok(response
            .into_iter()
            .map(|r| match r {
                RpcEither::Ok { result, .. } => Ok(result),
                RpcEither::Err { error, .. } => {
                    tracing::debug!("response {}", error.message);
                    Err(error)
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sorted_by(|a, b| a.number.cmp(&b.number))
            .collect())
    }

    #[tracing::instrument(level="info" skip_all, fields(number))]
    pub async fn block(&self, param: String) -> Result<Block, Error> {
        let request = serde_json::json!({
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [param, true],
        });
        let response: RpcEither<Block> = self
            .http_client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error {
                code: -1,
                message: format!("decoding block: {e:?}"),
            })?
            .json()
            .await
            .map_err(|e| Error {
                code: -1,
                message: format!("decoding block json: {e:?}"),
            })?;
        match response {
            RpcEither::Ok { result, .. } => Ok(result),
            RpcEither::Err { error, .. } => Err(error),
        }
    }

    #[tracing::instrument(level="info" skip_all, fields(from, to))]
    pub async fn logs(&self, from: u64, to: u64) -> Result<Vec<Log>, Error> {
        let request = serde_json::json!({
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": format!("0x{:x}", from),
                "toBlock":   format!("0x{:x}", to),
            }],
        });
        match self
            .http_client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error {
                code: -1,
                message: e.to_string(),
            })?
            .json::<RpcEither<Vec<Log>>>()
            .await
            .map_err(|e| Error {
                code: -1,
                message: format!("decoding logs: {e:?}"),
            })? {
            RpcEither::Ok { result, .. } => Ok(result),
            RpcEither::Err { error, .. } => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{b256, fixed_bytes, U64};

    #[test_log::test(tokio::test)]
    async fn test_batch() {
        let client = super::Client::new("https://rpc.flashbots.net");
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
                RpcEither::Err { error, .. } => {
                    eprintln!("#{i}: error {:?}", error);
                }
            }
        }
        assert!(!responses.is_empty(), "empty JSON response");
    }
}
