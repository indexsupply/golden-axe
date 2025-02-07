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
    pub id: String,
    pub card: Card,
}

#[derive(Debug, Serialize, Deserialize)]
struct PaymentIntent {
    pub customer: String,
    pub amount: i64,
    pub currency: String,
    pub payment_method: String,
    pub description: String,
    pub confirm: bool,
    pub off_session: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payment {
    pub id: String,
}

#[derive(Deserialize)]
struct List<T> {
    pub data: Vec<T>,
}

#[derive(Clone, Default)]
pub struct Client {
    key: Option<String>,
    reqwest: reqwest::Client,
}

#[derive(Debug, Deserialize)]
pub struct Session {
    pub id: String,
    pub url: Option<String>,
    pub customer: Option<String>,
}

impl Client {
    pub fn new(key: Option<String>) -> Client {
        Client {
            key,
            reqwest: reqwest::Client::new(),
        }
    }

    pub async fn create_session_update(
        &self,
        customer: &str,
        redirect_uri: &str,
    ) -> Result<Session> {
        self.post(
            "v1/checkout/sessions",
            &[
                ("mode", "setup"),
                ("customer", customer),
                ("currency", "usd"),
                ("success_url", redirect_uri),
            ],
        )
        .await
    }

    pub async fn create_session(&self, email: &str, redirect_uri: &str) -> Result<Session> {
        self.post(
            "v1/checkout/sessions",
            &[
                ("mode", "setup"),
                ("customer_email", email),
                ("customer_creation", "always"),
                ("success_url", redirect_uri),
                ("currency", "usd"),
            ],
        )
        .await
    }

    pub async fn get_session(&self, id: &str) -> Result<Option<Session>> {
        let resp = self
            .reqwest
            .get(format!(
                "https://api.stripe.com/v1/checkout/sessions/{}",
                id
            ))
            .basic_auth(self.key.as_ref().unwrap().to_string(), Some(""))
            .send()
            .await?;
        if resp.status().is_success() {
            let session = resp.json::<Session>().await?;
            println!("session: {:?}", session);
            Ok(Some(session))
        } else {
            Err(eyre!(resp.text().await?))
        }
    }

    pub async fn payment_methods(&self, customer_id: &str) -> Result<Option<PaymentMethod>> {
        if self.key.is_none() {
            return Ok(Some(PaymentMethod {
                id: String::from("LOCAL_DEV"),
                card: Card {
                    brand: String::from("LOCAL DEV"),
                    exp_month: 1,
                    exp_year: 2999,
                    last4: String::from("4242"),
                },
            }));
        }
        // Although the stripe docs do not specify, experimentation
        // has shown that the order id created_at desc
        let (path, data) = (
            format!("v1/customers/{}/payment_methods", customer_id),
            [("limit", 1)],
        );
        let res: List<PaymentMethod> = self.get(&path, &data).await?;
        Ok(res.data.into_iter().nth(0))
    }

    pub async fn charge_customer(
        &self,
        customer: String,
        description: String,
        amount: i64,
    ) -> Result<Payment> {
        match self.payment_methods(&customer).await {
            Err(_) => Err(eyre!("no payment method for {}", customer)),
            Ok(None) => Err(eyre!("no payment method for {}", customer)),
            Ok(Some(pm)) => {
                self.post(
                    "v1/payment_intents",
                    &PaymentIntent {
                        customer,
                        description,
                        payment_method: pm.id,
                        amount,
                        currency: String::from("usd"),
                        confirm: true,
                        off_session: true,
                    },
                )
                .await
            }
        }
    }

    pub async fn get<T: DeserializeOwned, D: Serialize + ?Sized>(
        &self,
        path: &str,
        data: &D,
    ) -> Result<T> {
        let request = self
            .reqwest
            .get(format!("https://api.stripe.com/{}", path))
            .basic_auth(self.key.as_ref().unwrap().to_string(), Some(""))
            .form(data);
        let response = request.send().await?;
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
            .basic_auth(
                self.key.as_ref().expect("missing stripe key").to_string(),
                Some(""),
            )
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
