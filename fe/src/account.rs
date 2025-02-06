use eyre::Result;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{daimo, stripe};

pub mod handlers {
    use axum::{
        extract::State,
        response::{Html, IntoResponse, Redirect},
    };
    use axum_extra::extract::{Form, SignedCookieJar};
    use serde_json::json;

    use crate::{
        api_key, query, session,
        web::{self, FlashMessage},
    };

    use super::{refresh_plan, Plan, PlanChangeRequest};

    pub async fn index(
        State(state): State<web::State>,
        flash: axum_flash::IncomingFlashes,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, shared::Error> {
        let user = session::User::from_jar(jar);
        let pg = state.pool.get().await?;
        let api_keys = if let Some(user) = &user {
            Some(api_key::list(&pg, &user.email).await?)
        } else {
            None
        };
        let history = if let Some(user) = &user {
            Some(query::user_history(&pg, &user.email).await?)
        } else {
            None
        };
        let resp = Html(state.templates.render(
            "index.html",
            &json!({
                "api_url": state.be_url,
                "api_keys": api_keys,
                "examples": state.examples,
                "history": history,
                "user": user,
                "flash": FlashMessage::from(flash.clone()),
            }),
        )?);
        Ok((flash, resp).into_response())
    }

    pub async fn account(
        State(state): State<web::State>,
        flash: axum_flash::IncomingFlashes,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, shared::Error> {
        let user = session::User::from_jar(jar).unwrap();
        let pg = state.pool.get().await?;
        let plan = refresh_plan(&state.daimo, &state.stripe, &pg, &user.email).await?;
        let api_keys = api_key::list(&pg, &user.email).await?;
        let rendered_html = state.templates.render(
            "account.html",
            &json!({
                "user": user,
                "flash": FlashMessage::from(flash.clone()),
                "plan": plan,
                "api_keys": api_keys,
            }),
        )?;
        Ok((flash, Html(rendered_html)).into_response())
    }

    pub async fn update_stripe(
        flash: axum_flash::Flash,
        State(state): State<web::State>,
        jar: SignedCookieJar,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let user = session::User::from_jar(jar).unwrap();
        let redirect = format!("{}/account", state.fe_url);
        let plan = Plan::get_latest_completed(&pg, &user.email).await?;
        if let Some(c) = plan.as_ref().and_then(|p| p.stripe_customer.clone()) {
            let session = state.stripe.create_session_update(&c, &redirect).await?;
            Ok(Redirect::to(&session.url.unwrap()).into_response())
        } else {
            let flash = flash.error("unable to find stripe plan for update");
            Ok((flash, Redirect::to("/account")).into_response())
        }
    }

    pub async fn setup_stripe(
        State(state): State<web::State>,
        jar: SignedCookieJar,
        Form(change): Form<PlanChangeRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let user = session::User::from_jar(jar).unwrap();
        let amount = change.stripe_amount();
        let redirect = format!("{}/account", state.fe_url);
        let session = state.stripe.create_session(&user.email, &redirect).await?;
        pg.execute(
            "
            insert into plan_changes (owner_email, name, amount, stripe_session)
            values ($1, $2, $3, $4)
            ",
            &[&user.email, &change.plan_name, &amount, &session.id],
        )
        .await?;
        Ok(Redirect::to(&session.url.unwrap()))
    }

    pub async fn setup_daimo(
        State(state): State<web::State>,
        jar: SignedCookieJar,
        Form(change): Form<PlanChangeRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let user = session::User::from_jar(jar).unwrap();
        let plan = Plan::get_latest_completed(&pg, &user.email).await?;
        let amount = change.daimo_amount(plan.map(|p| p.balance()));
        let redirect = &format!("{}/account", state.fe_url);
        let payment_link = state
            .daimo
            .generate(&change.plan_name, 100, redirect)
            .await?;
        pg.execute(
            "
            insert into plan_changes (owner_email, name, amount, daimo_id)
            values ($1, $2, $3, $4)
            ",
            &[&user.email, &change.plan_name, &amount, &payment_link.id],
        )
        .await?;
        Ok(Redirect::to(&payment_link.url))
    }
}

pub async fn refresh_plan(
    daimo: &daimo::Client,
    stripe: &stripe::Client,
    pg: &tokio_postgres::Client,
    email: &str,
) -> Result<Option<Plan>, shared::Error> {
    check_daimo(email, daimo, pg).await?;
    check_stripe(email, stripe, pg).await?;
    Plan::get_latest(pg, email).await
}

async fn check_daimo(
    email: &str,
    daimo: &daimo::Client,
    pg: &tokio_postgres::Client,
) -> Result<(), shared::Error> {
    let res = pg
        .query(
            "
            select id, daimo_id
            from plan_changes
            where owner_email = $1
            and daimo_id is not null
            and daimo_tx is null
            order by created_at
            limit 1
            ",
            &[&email],
        )
        .await?;
    if let Some(row) = res.first() {
        if let Some(tx_hash) = daimo.check(row.get("daimo_id")).await? {
            pg.execute(
                "update plan_changes set daimo_tx = $1 where id = $2",
                &[&tx_hash, &row.get::<&str, i64>("id")],
            )
            .await?;
        }
    }
    Ok(())
}

async fn check_stripe(
    email: &str,
    stripe: &stripe::Client,
    pg: &tokio_postgres::Client,
) -> Result<(), shared::Error> {
    let res = pg
        .query(
            "
            select id, stripe_session
            from plan_changes
            where owner_email = $1
            and stripe_session is not null
            and stripe_customer is null
            order by created_at
            limit 1
            ",
            &[&email],
        )
        .await?;
    if let Some(row) = res.first() {
        if let Some(session) = stripe.get_session(row.get("stripe_session")).await? {
            pg.execute(
                "update plan_changes set stripe_customer = $1 where id = $2",
                &[
                    &session.customer.unwrap_or_default(),
                    &row.get::<&str, i64>("id"),
                ],
            )
            .await?;
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct PlanChangeRequest {
    plan_name: String,
}

impl PlanChangeRequest {
    /// returns the plan's base amount less any previously paid balance
    fn daimo_amount(&self, balance: Option<i64>) -> i64 {
        let base = match self.plan_name.as_str() {
            "indie" => 40000,
            "pro" => 280000,
            "unlimited" => 2200000,
            _ => 0,
        };
        base - balance.unwrap_or(0)
    }

    fn stripe_amount(&self) -> i64 {
        match self.plan_name.as_str() {
            "indie" => 5000,
            "pro" => 25000,
            "unlimited" => 200000,
            _ => 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Plan {
    id: i64,
    owner_email: Option<String>,
    name: String,
    amount: i64,
    daimo_id: Option<String>,
    daimo_tx: Option<String>,
    stripe_session: Option<String>,
    stripe_customer: Option<String>,
    created_at: OffsetDateTime,
}

impl Plan {
    fn balance(&self) -> i64 {
        self.daimo_tx.as_ref().map_or(0, |_| {
            let remaining = 365 - (OffsetDateTime::now_utc() - self.created_at).whole_days();
            remaining.max(0) * (self.amount / 365)
        })
    }

    fn from_row(row: &tokio_postgres::Row) -> Plan {
        Plan {
            owner_email: row.get("owner_email"),
            id: row.get("id"),
            name: row.get("name"),
            amount: row.get("amount"),
            daimo_id: row.get("daimo_id"),
            daimo_tx: row.get("daimo_tx"),
            stripe_session: row.get("stripe_session"),
            stripe_customer: row.get("stripe_customer"),
            created_at: row.get("created_at"),
        }
    }

    pub async fn get_latest_completed(
        pg: &tokio_postgres::Client,
        email: &str,
    ) -> Result<Option<Plan>, shared::Error> {
        Ok(pg
            .query(
                "
                select owner_email, id, name, amount, daimo_id, daimo_tx, stripe_session, stripe_customer, created_at
                from plan_changes
                where owner_email = $1
                and (
                    (daimo_id is not null and daimo_tx is not null)
                    or
                    (stripe_session is not null and stripe_customer is not null)
                )
                order by created_at desc
                limit 1
                ",
                &[&email],
            )
            .await?
            .first()
            .map(Plan::from_row))
    }

    pub async fn get_latest(
        pg: &tokio_postgres::Client,
        email: &str,
    ) -> Result<Option<Plan>, shared::Error> {
        Ok(pg
            .query(
                "
                select owner_email, id, name, amount, daimo_id, daimo_tx, stripe_session, stripe_customer, created_at
                from plan_changes
                where owner_email = $1
                order by created_at desc
                limit 1
                ",
                &[&email],
            )
            .await?.first().map(Plan::from_row))
    }
}
