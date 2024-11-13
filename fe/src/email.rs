use eyre::{eyre, Result};
use serde::Deserialize;

#[derive(Clone)]
pub struct Client {
    pub site_url: String,
    pub key: String,
    pub validation_key: String,
}

#[derive(Deserialize)]
struct ValidationResponse {
    result: ValidationResult,
}

#[derive(Deserialize)]
struct ValidationResult {
    verdict: String,
    score: f32,
}

impl Client {
    async fn validate_email(&self, to: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let response = client
            .post("https://api.sendgrid.com/v3/validations/email")
            .header("Authorization", format!("Bearer {}", self.validation_key))
            .json(&serde_json::json!({"email": to}))
            .send()
            .await?;
        if response.status().is_success() {
            let vresp = response.json::<ValidationResponse>().await?;
            if vresp.result.verdict == "Valid" || vresp.result.score > 0.1 {
                Ok(())
            } else {
                Err(eyre!("validating email: {}", to))
            }
        } else {
            let resp = response.text().await?;
            tracing::info!("invalid email to {} failed: {}", to, resp);
            Err(eyre!("validating email: {}", resp))
        }
    }
    pub async fn send_email_login(&self, to: &str, secret: Vec<u8>) -> Result<()> {
        self.validate_email(to).await?;
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
