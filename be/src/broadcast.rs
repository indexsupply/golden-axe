use serde::Serialize;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize)]
pub struct NewBlock {
    pub src: String,
    pub chain: u64,
    pub num: u64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum Message {
    Block(NewBlock),
    Json(serde_json::Value),
    Close,
}

pub struct Channel {
    clients: broadcast::Sender<Message>,
}

impl Default for Channel {
    fn default() -> Self {
        Self {
            clients: broadcast::channel(16).0,
        }
    }
}

impl Channel {
    pub fn wait(&self) -> broadcast::Receiver<Message> {
        self.clients.subscribe()
    }

    pub fn update(&self, msg: Message) {
        let _ = self.clients.send(msg);
    }

    pub fn new_block(&self, src: &str, chain: u64, num: u64) {
        let _ = self.clients.send(Message::Block(NewBlock {
            src: src.to_string(),
            chain,
            num,
        }));
    }

    pub fn close(&self) {
        let _ = self.clients.send(Message::Close);
    }
}
