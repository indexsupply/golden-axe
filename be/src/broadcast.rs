use dashmap::DashMap;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::broadcast::{self, error::RecvError};

pub struct Channel {
    pub json_updates: broadcast::Sender<serde_json::Value>,
    pub block_updates: DashMap<u64, broadcast::Sender<()>>,
}

impl Default for Channel {
    fn default() -> Self {
        Self {
            json_updates: broadcast::channel(16).0,
            block_updates: DashMap::new(),
        }
    }
}

impl Channel {
    fn subscribe(&self, chain_ids: &[u64]) -> Vec<(u64, broadcast::Receiver<()>)> {
        chain_ids
            .iter()
            .map(|&chain| {
                let rx = self
                    .block_updates
                    .entry(chain)
                    .or_insert_with(|| broadcast::channel(16).0)
                    .subscribe();
                (chain, rx)
            })
            .collect()
    }

    pub fn update(&self, chain: u64) {
        let sender = self
            .block_updates
            .entry(chain)
            .or_insert_with(|| broadcast::channel(16).0);
        let _ = sender.send(());
    }

    pub async fn wait(&self, chain_ids: &[u64]) -> Option<u64> {
        let mut futs = self
            .subscribe(chain_ids)
            .into_iter()
            .map(|(chain, mut rx)| {
                tokio::spawn(async move {
                    match rx.recv().await {
                        Ok(_) => Some(chain),
                        Err(RecvError::Lagged(skipped)) => {
                            tracing::warn!(chain, skipped, "receiver lagged");
                            None
                        }
                        Err(RecvError::Closed) => {
                            tracing::error!(chain, "receiver closed");
                            None
                        }
                    }
                })
            })
            .collect::<FuturesUnordered<_>>();
        futs.next().await.and_then(|res| res.ok().flatten())
    }
}
