use eyre::{eyre, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Client {
    key: Option<String>,
    client: reqwest::Client,
}

#[derive(Deserialize)]
struct Response {
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "ErrorCode")]
    error_code: i32,
}

impl Client {
    pub fn new(key: Option<String>) -> Client {
        Client {
            key,
            client: reqwest::Client::new(),
        }
    }

    pub async fn send(&self, from: &str, to: &str, subject: &str, body: &str) -> Result<()> {
        self.post(&serde_json::json!({
            "From": from,
            "To": to,
            "ReplyTo": "support@indexsupply.net",
            "Subject": subject,
            "TextBody": body,
            "MessageStream": "outbound"
        }))
        .await
    }

    async fn post<D: Serialize + ?Sized>(&self, data: &D) -> Result<()> {
        if self.key.is_none() {
            let body = serde_json::to_string_pretty(&data)?;
            tracing::info!("postmark key missing. skipping email. content: {}", body);
            return Ok(());
        }
        let response: Response = self
            .client
            .post("https://api.postmarkapp.com/email")
            .header(
                "X-Postmark-Server-Token",
                self.key.as_ref().unwrap().to_string(),
            )
            .json(&data)
            .send()
            .await?
            .json()
            .await?;
        if response.error_code == 0 {
            Ok(())
        } else {
            Err(eyre!("sending email: {}", response.message))
        }
    }
}
