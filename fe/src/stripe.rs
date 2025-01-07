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

impl Client {
    pub fn new(key: Option<String>) -> Client {
        Client {
            key,
            reqwest: reqwest::Client::new(),
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

    pub async fn setup_intent(&self, customer_id: &str) -> Result<SetupIntent> {
        if self.key.is_none() {
            return Ok(SetupIntent {
                client_secret: String::from("LOCAL DEV"),
            });
        }
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
        if self.key.is_none() {
            return Ok(Customer {
                id: String::from("LOCAL DEV"),
                email: String::from("local@dev"),
            });
        }
        let (path, data) = ("v1/customers", [("email", email)]);
        self.post(path, &data).await
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
        let response = self
            .reqwest
            .get(format!("https://api.stripe.com/{}", path))
            .basic_auth(self.key.as_ref().unwrap().to_string(), Some(""))
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
