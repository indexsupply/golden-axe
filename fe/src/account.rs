use eyre::{OptionExt, Result};

use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

use crate::{daimo, stripe};

time::serde::format_description!(short, OffsetDateTime, "[year]-[month]-[day]");

pub mod handlers {

    use axum::{
        extract::State,
        response::{Html, IntoResponse, Redirect},
    };
    use axum_extra::extract::Form;
    use itertools::Itertools;
    use serde::Deserialize;
    use serde_json::json;

    use crate::{
        account::{view_plan_options, PlanOption},
        api_key, chains, session,
        web::{self, FlashMessage},
    };

    use super::{refresh_plan, PlanChange, PlanChangeRequest};

    pub async fn index(
        State(state): State<web::State>,
        flash: axum_flash::IncomingFlashes,
        user: Option<session::User>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let api_keys = if let Some(user) = &user {
            Some(api_key::list(&pg, &user.email).await?)
        } else {
            None
        };
        let chains = chains::list(&pg)
            .await?
            .into_iter()
            .sorted_by_key(|c| c.name.to_string())
            .filter(|c| c.enabled)
            .collect::<Vec<_>>();
        let resp = Html(state.templates.render(
            "index.html",
            &json!({
                "api_url": state.be_url,
                "api_keys": api_keys,
                "chains": chains,
                "examples": state.examples,
                "user": user,
                "flash": FlashMessage::from(flash.clone()),
            }),
        )?);
        Ok((flash, resp).into_response())
    }

    pub async fn account(
        State(state): State<web::State>,
        flash: axum_flash::IncomingFlashes,
        user: session::User,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let plan = refresh_plan(&state.daimo, &state.stripe, &pg, &user.email).await?;
        let api_keys = api_key::list(&pg, &user.email).await?;
        let usage = super::usage(&pg, &user.email).await?;
        let rendered_html = state.templates.render(
            "account.html",
            &json!({
                "user": user,
                "flash": FlashMessage::from(flash.clone()),
                "plan": plan,
                "api_keys": api_keys,
                "usage": usage,
                "options": view_plan_options(&pg, &user.email).await?,
            }),
        )?;
        Ok((flash, Html(rendered_html)).into_response())
    }

    #[derive(Deserialize)]
    pub struct UpdateHardLimitRequest {
        pub enabled: bool,
    }

    pub async fn update_limit(
        flash: axum_flash::Flash,
        State(state): State<web::State>,
        user: session::User,
        Form(req): Form<UpdateHardLimitRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let res = pg
            .execute(
                "
                update plan_changes
                set hard_limit = $1
                where id = (
                    select id from plan_changes
                    where owner_email = $2
                    order by created_at desc
                    limit 1
                );
                ",
                &[&req.enabled, &user.email],
            )
            .await?;
        if res == 1 {
            if req.enabled {
                let flash = flash.success("Hard limit enabaled");
                Ok((flash, Redirect::to("/account")).into_response())
            } else {
                let flash = flash.success("Hard limit disabled");
                Ok((flash, Redirect::to("/account")).into_response())
            }
        } else {
            tracing::error!("unable to update account for {}", &user.email);
            let flash = flash.error("Unable to update account");
            Ok((flash, Redirect::to("/account")).into_response())
        }
    }

    pub async fn update_stripe(
        flash: axum_flash::Flash,
        State(state): State<web::State>,
        user: session::User,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let redirect = format!("{}/account", state.fe_url);
        let plan = PlanChange::get_latest_completed(&pg, &user.email).await?;
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
        user: session::User,
        Form(change): Form<PlanChangeRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let new_plan = PlanOption::get(&pg, &change.plan_name, &user.email).await?;
        let redirect = format!("{}/account", state.fe_url);
        let session = state.stripe.create_session(&user.email, &redirect).await?;
        pg.execute(
            "
            insert into plan_changes (owner_email, name, rate, timeout, connections, queries, amount, stripe_session)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            ",
            &[
                &user.email,
                &new_plan.name,
                &new_plan.rate,
                &new_plan.timeout,
                &new_plan.connections,
                &new_plan.queries,
                &new_plan.stripe_amount,
                &session.id,
            ],
        )
        .await?;
        Ok(Redirect::to(&session.url.unwrap()))
    }

    pub async fn setup_daimo(
        State(state): State<web::State>,
        user: session::User,
        Form(change): Form<PlanChangeRequest>,
    ) -> Result<impl IntoResponse, shared::Error> {
        let pg = state.pool.get().await?;
        let current_plan = PlanChange::get_latest_completed(&pg, &user.email).await?;
        let new_plan = PlanOption::get(&pg, &change.plan_name, &user.email).await?;
        let amount = if let Some(p) = current_plan {
            new_plan.daimo_amount - p.balance()
        } else {
            new_plan.daimo_amount
        };
        let redirect = &format!("{}/account", state.fe_url);
        let payment_link = state
            .daimo
            .generate(&new_plan.name, amount, redirect)
            .await?;
        pg.execute(
            "
            insert into plan_changes (owner_email, name, rate, timeout, connections, queries, amount, daimo_id)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            ",
            &[
                &user.email,
                &new_plan.name,
                &new_plan.rate,
                &new_plan.timeout,
                &new_plan.connections,
                &new_plan.queries,
                &amount,
                &payment_link.id,
            ],
        )
        .await?;
        Ok(Redirect::to(&payment_link.url))
    }
}

pub async fn usage(pg: &tokio_postgres::Client, email: &str) -> Result<i64, shared::Error> {
    let rows = pg
        .query(
            "
            select coalesce(sum(n)::int8, 0) as n
            from daily_user_queries
            where day >= date_trunc('month', now())::date
            and day < date_trunc('month', now() + interval '1 month')::date
            and owner_email = $1
            ",
            &[&email],
        )
        .await?;
    Ok(rows.first().map(|r| r.get("n")).unwrap_or(0))
}

pub async fn refresh_plan(
    daimo: &daimo::Client,
    stripe: &stripe::Client,
    pg: &tokio_postgres::Client,
    email: &str,
) -> Result<Option<PlanChange>, shared::Error> {
    check_daimo(email, daimo, pg).await?;
    check_stripe(email, stripe, pg).await?;
    PlanChange::get_latest(pg, email).await
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
            order by created_at desc
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

async fn view_plan_options(
    pg: &tokio_postgres::Client,
    email: &str,
) -> Result<Vec<PlanOptionView>, shared::Error> {
    let mut opts = PlanOption::all(pg, email).await?;
    let latest_plan = PlanChange::get_latest_completed(pg, email).await?;
    Ok(opts
        .iter_mut()
        .map(|o| PlanOptionView {
            name: o.name.clone(),
            features: o.features.clone(),
            daimo_amount: match &latest_plan {
                None => money(o.daimo_amount),
                Some(p) => money(o.daimo_amount - p.balance()),
            },
            daimo_monthly: match &latest_plan {
                None => money(o.daimo_amount / 12),
                Some(p) => money((o.daimo_amount - p.balance()) / 12),
            },
            stripe_amount: money(o.stripe_amount),
            show_purchase_button: match &latest_plan {
                None => true,
                Some(p) => p.name != o.name,
            },
        })
        .collect())
}

#[derive(Serialize, Deserialize)]
pub struct PlanChangeRequest {
    plan_name: String,
}

#[derive(Clone, Debug, Serialize)]
struct PlanOption {
    name: String,
    features: Vec<String>,
    rate: i32,
    timeout: i32,
    connections: i32,
    queries: i32,
    owner_email: Option<String>,
    daimo_amount: i64,
    stripe_amount: i64,
}

impl PlanOption {
    async fn get(pg: &tokio_postgres::Client, name: &str, owner_email: &str) -> Result<PlanOption> {
        pg.query(
            "
                select name, features, rate, timeout, connections, queries, owner_email, daimo_amount, stripe_amount
                from plan_options
                where name = $1 and (owner_email is null or owner_email = $2)
                ",
            &[&name, &owner_email],
        )
        .await?
        .first()
        .map(|row| PlanOption {
            name: row.get("name"),
            features: row.get("features"),
            rate: row.get("rate"),
            timeout: row.get("timeout"),
            connections: row.get("connections"),
            queries: row.get("queries"),
            owner_email: row.get("owner_email"),
            daimo_amount: row.get("daimo_amount"),
            stripe_amount: row.get("stripe_amount"),
        })
        .ok_or_eyre("missing plan")
    }

    async fn all(pg: &tokio_postgres::Client, owner_email: &str) -> Result<Vec<PlanOption>> {
        let all_plans: Vec<PlanOption> = pg
            .query(
                "
                select name, features, rate, timeout, connections, queries, owner_email, daimo_amount, stripe_amount
                from plan_options
                where owner_email is null or owner_email = $1
                order by daimo_amount, stripe_amount asc
                ",
                &[&owner_email],
            )
            .await?
            .iter()
            .map(|row| PlanOption {
                name: row.get("name"),
                features: row.get("features"),
                rate: row.get("rate"),
                timeout: row.get("timeout"),
                connections: row.get("connections"),
                queries: row.get("queries"),
                owner_email: row.get("owner_email"),
                daimo_amount: row.get("daimo_amount"),
                stripe_amount: row.get("stripe_amount"),
            })
            .collect();
        let mut plans: Vec<PlanOption> = all_plans
            .iter()
            .filter(|p| p.name == "Indie" || p.name == "Pro" || p.owner_email.is_some())
            .cloned()
            .collect();
        if plans.len() == 2 {
            if let Some(plan) = all_plans.iter().find(|p| p.name == "Dedicated") {
                plans.push(plan.clone());
            }
        }
        Ok(plans)
    }
}

#[derive(Serialize)]
struct PlanOptionView {
    name: String,
    features: Vec<String>,
    daimo_amount: String,
    daimo_monthly: String,
    stripe_amount: String,
    show_purchase_button: bool,
}

pub fn money(number_pennies: i64) -> String {
    let amount = (Decimal::from(number_pennies) / dec!(100))
        .round_dp(2)
        .max(dec!(0));
    format!("${}", with_commas(amount))
}

pub fn with_commas(num: Decimal) -> String {
    let num_str = num.to_string();
    let (left, right) = num_str.split_once('.').unwrap_or((&num_str, ""));
    let formatted = left
        .chars()
        .collect::<Vec<_>>()
        .rchunks(3)
        .rev()
        .map(|c| c.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join(",");

    if right.is_empty() {
        formatted
    } else {
        format!("{formatted}.{right}")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlanChange {
    id: i64,
    owner_email: Option<String>,
    name: String,
    rate: i32,
    timeout: i32,
    connections: i32,
    queries: i32,
    hard_limit: bool,
    amount: i64,
    daimo_id: Option<String>,
    daimo_tx: Option<String>,
    stripe_session: Option<String>,
    stripe_customer: Option<String>,
    created_at: OffsetDateTime,
    #[serde(skip_deserializing, with = "short::option")]
    expiration: Option<OffsetDateTime>,
    card: Option<String>,
}

impl PlanChange {
    fn balance(&self) -> i64 {
        self.daimo_tx.as_ref().map_or(0, |_| {
            let remaining = 365 - (OffsetDateTime::now_utc() - self.created_at).whole_days();
            let daily_amount = Decimal::from(self.amount) / dec!(365);
            let balance = Decimal::from(remaining) * daily_amount;
            balance.to_i64().unwrap_or(0).max(0)
        })
    }

    fn from_row(row: &tokio_postgres::Row) -> PlanChange {
        let mut plan = PlanChange {
            owner_email: row.get("owner_email"),
            id: row.get("id"),
            name: row.get("name"),
            rate: row.get("rate"),
            timeout: row.get("timeout"),
            connections: row.get("connections"),
            queries: row.get("queries"),
            hard_limit: row.get("hard_limit"),
            amount: row.get("amount"),
            daimo_id: row.get("daimo_id"),
            daimo_tx: row.get("daimo_tx"),
            stripe_session: row.get("stripe_session"),
            stripe_customer: row.get("stripe_customer"),
            created_at: row.get("created_at"),
            expiration: None,
            card: None,
        };
        if plan.daimo_tx.is_some() {
            plan.expiration = plan.created_at.checked_add(Duration::days(365));
        }
        plan
    }

    pub async fn get_latest_completed(
        pg: &tokio_postgres::Client,
        email: &str,
    ) -> Result<Option<PlanChange>, shared::Error> {
        Ok(pg
            .query(
                "
                select owner_email, id, name, rate, timeout, connections, queries, hard_limit, amount, daimo_id, daimo_tx, stripe_session, stripe_customer, created_at
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
            .map(PlanChange::from_row))
    }

    pub async fn get_latest(
        pg: &tokio_postgres::Client,
        email: &str,
    ) -> Result<Option<PlanChange>, shared::Error> {
        Ok(pg
            .query(
                "
                select owner_email, id, name, rate, timeout, connections, queries, hard_limit, amount, daimo_id, daimo_tx, stripe_session, stripe_customer, created_at
                from plan_changes
                where owner_email = $1
                order by created_at desc
                limit 1
                ",
                &[&email],
            )
            .await?.first().map(PlanChange::from_row))
    }
}
