use eyre::{eyre, Result};

#[derive(Clone)]
pub struct Client {
    pub site_url: String,
    pub key: String,
}

impl Client {
    pub async fn send_email_login(&self, to: &str, secret: Vec<u8>) -> Result<()> {
        let client = reqwest::Client::new();
        let body = format!(
            "Click to log in: {}/email-login-link?secret={}",
            self.site_url,
            hex::encode(secret),
        );
        let request = serde_json::json!({
            "personalizations": [{"to": [{"email": to}]}],
            "from": {"email": "support@indexsupply.com"},
            "subject": "Index Supply Log In Link",
            "content": [{"type": "text/plain", "value":body}]
        });
        let response = client
            .post("https://api.sendgrid.com/v3/mail/send")
            .header("Authorization", format!("Bearer {}", self.key))
            .json(&request)
            .send()
            .await?;
        if response.status().is_success() {
            tracing::info!("login email sent to: {}", to);
            Ok(())
        } else {
            let resp = response.text().await?;
            tracing::info!("login email to {} failed: {}", to, resp);
            Err(eyre!("sending email: {}", resp))
        }
    }
}
