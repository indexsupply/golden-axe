use eyre::{eyre, Result};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Customer {
    pub id: String,
    pub email: String,
}

#[derive(Clone)]
pub struct Client {
    key: String,
    reqwest: reqwest::Client,
}

impl Client {
    pub fn new(key: &str) -> Client {
        Client {
            reqwest: reqwest::Client::new(),
            key: key.to_string(),
        }
    }

    // It's sad that you cannot provide an id for stripe
    // If you call this multiple times you will end up with
    // multiple stripe customers with the same email.
    //
    // The stripe customer search is not in-sync with the customer create
    // endpoint meaning that you can't rely on searching for a customer
    // before creating one.
    pub async fn create_customer(&self, email: &str) -> Result<Customer> {
        let response = self
            .reqwest
            .post("https://api.stripe.com/v1/customers")
            .basic_auth(&self.key, Some(""))
            .query(&[("email", email)])
            .send()
            .await?;
        if response.status().is_success() {
            let customer: Customer = response.json().await?;
            tracing::info!("stripe customer created email");
            Ok(customer)
        } else {
            let resp = response.text().await?;
            Err(eyre!(
                "creating stripe customer: {}. response: {}",
                email,
                resp
            ))
        }
    }
}
