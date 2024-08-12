use eyre::{eyre, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Deserialize)]
pub struct Customer {
    pub id: String,
    pub email: String,
}

#[derive(Serialize, Deserialize)]
pub struct SetupIntent {
    pub client_secret: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Card {
    pub brand: String,
    pub exp_month: u64,
    pub exp_year: u64,
    pub last4: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PaymentMethod {
    pub card: Card,
}

#[derive(Deserialize)]
struct List<T> {
    pub data: Vec<T>,
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

    pub async fn payment_methods(&self, customer_id: &str) -> Result<Option<PaymentMethod>> {
        // Although the stripe docs do not specify, experimentation
        // has shown that the order id created_at desc
        let (path, data) = (
            format!("v1/customers/{}/payment_methods", customer_id),
            [("limit", 1)],
        );
        let res: List<PaymentMethod> = self.get(&path, &data).await?;
        Ok(res.data.into_iter().nth(0))
    }

    pub async fn setup_intent(&self, customer_id: &str) -> Result<SetupIntent> {
        let (path, data) = ("v1/setup_intents", [("customer", customer_id)]);
        self.post(path, &data).await
    }

    // It's sad that you cannot provide an id for stripe
    // If you call this multiple times you will end up with
    // multiple stripe customers with the same email.
    //
    // The stripe customer search is not in-sync with the customer create
    // endpoint meaning that you can't rely on searching for a customer
    // before creating one.
    pub async fn create_customer(&self, email: &str) -> Result<Customer> {
        let (path, data) = ("v1/customers", [("email", email)]);
        self.post(path, &data).await
    }

    pub async fn get<T: DeserializeOwned, D: Serialize + ?Sized>(
        &self,
        path: &str,
        data: &D,
    ) -> Result<T> {
        let response = self
            .reqwest
            .get(format!("https://api.stripe.com/{}", path))
            .basic_auth(&self.key, Some(""))
            .form(data)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let resp = response.text().await?;
            Err(eyre!("making stripe request: {} {}", path, resp))
        }
    }

    pub async fn post<T: DeserializeOwned, D: Serialize + ?Sized>(
        &self,
        path: &str,
        data: &D,
    ) -> Result<T> {
        let response = self
            .reqwest
            .post(format!("https://api.stripe.com/{}", path))
            .basic_auth(&self.key, Some(""))
            .form(data)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let resp = response.text().await?;
            Err(eyre!("making stripe request: {} {}", path, resp))
        }
    }
}
