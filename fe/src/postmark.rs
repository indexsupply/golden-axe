use eyre::{eyre, Result};
use serde::Deserialize;

#[derive(Clone)]
pub struct Client {
    site_url: String,
    key: String,
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
    pub fn new(key: String, site_url: String) -> Client {
        Client {
            key,
            site_url,
            client: reqwest::Client::new(),
        }
    }
    pub async fn send_email_login(&self, to: &str, secret: Vec<u8>) -> Result<()> {
        let body = format!(
            "Click to log in: {}/email-login-link?secret={}",
            self.site_url,
            hex::encode(secret),
        );
        let request = serde_json::json!({
            "From": "login@indexsupply.net",
            "To": to,
            "Subject": "Index Supply Log In Link",
            "TextBody": body,
            "MessageStream": "outbound"
        });
        let response: Response = self
            .client
            .post("https://api.postmarkapp.com/email")
            .header("X-Postmark-Server-Token", self.key.to_string())
            .json(&request)
            .send()
            .await?
            .json()
            .await?;
        if response.error_code == 0 {
            tracing::info!("login email sent to: {}", to);
            Ok(())
        } else {
            tracing::info!("login email to {} failed: {}", to, response.message);
            Err(eyre!("sending email: {}", response.message))
        }
    }
}
