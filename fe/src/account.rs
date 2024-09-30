use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{stripe, web};

pub mod handlers {
    use axum::{
        extract::State,
        response::{Html, IntoResponse},
        Json,
    };
    use axum_extra::extract::SignedCookieJar;
    use serde_json::json;

    use crate::{
        api_key, session,
        web::{self, FlashMessage},
    };

    use super::{current_plan, setup_stripe, Plan, PlanChangeRequest};

    pub async fn index(
        State(state): State<web::State>,
        flash: axum_flash::IncomingFlashes,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, web::Error> {
        let user = session::User::from_jar(jar);
        let resp = Html(state.templates.render(
            "index",
            &json!({
                "api_url": state.api_url,
                "user": user,
                "flash": FlashMessage::from(flash.clone()),
            }),
        )?);
        Ok((flash, resp).into_response())
    }

    pub async fn change_plan(
        State(state): State<web::State>,
        flash: axum_flash::Flash,
        jar: SignedCookieJar,
        Json(change): Json<PlanChangeRequest>,
    ) -> Result<impl IntoResponse, web::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let mut plan: Plan = change.into();
        plan.owner_email = Some(user.email);

        let mut pg = state.pool.get().await?;
        let mut pgtx = pg.transaction().await?;
        plan.insert(&mut pgtx).await?;
        pgtx.commit().await?;

        let flash = if &plan.name == "pro" {
            flash.success("⚡️upgraded your plan to: PRO⚡️")
        } else {
            flash.success(format!("changed your plan to: {}", &plan.name))
        };
        Ok((flash, axum::http::StatusCode::OK).into_response())
    }

    pub async fn account(
        State(state): State<web::State>,
        flash: axum_flash::IncomingFlashes,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, web::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let mut pg = state.pool.get().await?;
        let customer_id = setup_stripe(&mut pg, &state.stripe, &user.email).await?;
        let intent = state.stripe.setup_intent(&customer_id).await?;
        let payment_method = state.stripe.payment_methods(&customer_id).await?;
        let plan = current_plan(&pg, &user.email).await?;
        let api_keys = api_key::list(&pg, &user.email).await?;
        let rendered_html = state.templates.render(
            "account",
            &json!({
                "user": user,
                "flash": FlashMessage::from(flash.clone()),
                "stripe_pub_key": state.stripe_pub_key.to_string(),
                "client_secret": intent.client_secret.to_string(),
                "plan": plan,
                "payment_method": payment_method,
                "api_keys": api_keys,
            }),
        )?;
        Ok((flash, Html(rendered_html)).into_response())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Plan {
    owner_email: Option<String>,
    name: String,
    rate: i32,
    timeout: i32,
    chains: Vec<i64>,
}

impl Plan {
    fn default() -> Plan {
        PlanChangeRequest {
            name: String::from("indie"),
            chains: vec![8453, 84532],
        }
        .into()
    }
    async fn insert(&self, pgtx: &mut tokio_postgres::Transaction<'_>) -> Result<(), web::Error> {
        pgtx.query(
            "
                insert into plan_changes (owner_email, name, rate, timeout, chains)
                values ($1, $2, $3, $4, $5)
            ",
            &[
                &self.owner_email,
                &self.name,
                &self.rate,
                &self.timeout,
                &self.chains,
            ],
        )
        .await?;
        Ok(())
    }
}

impl From<PlanChangeRequest> for Plan {
    fn from(change: PlanChangeRequest) -> Self {
        let (rps, ttl): (i32, i32) = match change.name.to_lowercase().as_str() {
            "indie" => (10, 10),
            "pro" => (100, 60),
            "dedicated" => (100, 60),
            _ => (0, 0),
        };
        Plan {
            owner_email: None,
            name: change.name,
            rate: rps,
            timeout: ttl,
            chains: change.chains,
        }
    }
}

#[derive(Deserialize)]
pub struct PlanChangeRequest {
    name: String,
    chains: Vec<i64>,
}

pub async fn current_plan(
    pg: &tokio_postgres::Client,
    email: &str,
) -> Result<Option<Plan>, web::Error> {
    let res = pg
        .query(
            "
            select name, rate, timeout, chains
            from plan_changes
            where owner_email = $1
            order by created_at desc
            limit 1
            ",
            &[&email],
        )
        .await?;
    if res.is_empty() {
        Ok(None)
    } else {
        let row = res.first().expect("should be at leaset 1 plan_change");
        Ok(Some(Plan {
            owner_email: None,
            name: row.get("name"),
            rate: row.get("rate"),
            timeout: row.get("timeout"),
            chains: row.get("chains"),
        }))
    }
}

async fn setup_stripe(
    pg: &mut tokio_postgres::Client,
    stripe: &stripe::Client,
    email: &str,
) -> Result<String, web::Error> {
    let mut pgtx = pg.transaction().await?;
    let res = pgtx
        .query(
            "select stripe_id from accounts where owner_email = $1",
            &[&email],
        )
        .await?;
    if res.is_empty() {
        tracing::debug!("creating stripe customer for: {}", email);
        let customer = stripe.create_customer(email).await?;
        pgtx.execute(
            "insert into accounts (owner_email, stripe_id) values ($1, $2)",
            &[&email, &customer.id],
        )
        .await?;
        let mut plan = Plan::default();
        plan.owner_email = Some(email.to_string());
        plan.insert(&mut pgtx).await?;
        pgtx.commit().await?;
        Ok(customer.id)
    } else {
        let strip_id: String = res.first().unwrap().get(0);
        tracing::debug!("stripe customer exists for {}", email);
        Ok(strip_id)
    }
}
