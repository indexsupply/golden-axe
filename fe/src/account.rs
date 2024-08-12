use axum::{
    extract::State,
    response::{Html, IntoResponse},
    Json,
};
use axum_extra::extract::SignedCookieJar;
use eyre::Result;
use serde_json::json;

use crate::{
    session, stripe,
    web::{self, FlashMessage},
};

pub async fn index(
    State(state): State<web::State>,
    flash: axum_flash::IncomingFlashes,
    jar: SignedCookieJar,
) -> Result<impl IntoResponse, web::Error> {
    let user = session::User::from_jar(jar);
    let resp = Html(state.templates.render(
        "index",
        &json!({
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
    Json(new_plan): Json<String>,
) -> Result<impl IntoResponse, web::Error> {
    let pg = state.pool.get().await?;
    let user = session::User::from_jar(jar).unwrap();
    pg.query(
        "insert into plan_changes (owner_email, name) values ($1, $2)",
        &[&user.email, &new_plan],
    )
    .await?;
    let flash = if new_plan == "extreme" {
        flash.success("⚡️upgraded your plan to: EXTREME⚡️")
    } else {
        flash.success(format!("changed your plan to: {}", new_plan))
    };
    Ok((flash, axum::http::StatusCode::OK).into_response())
}

pub async fn account(
    State(state): State<web::State>,
    flash: axum_flash::IncomingFlashes,
    jar: SignedCookieJar,
) -> Result<impl IntoResponse, web::Error> {
    let pg = state.pool.get().await?;
    let user = session::User::from_jar(jar).unwrap();
    let customer_id = setup_stripe(&pg, &state.stripe, &user.email).await?;
    let intent = state.stripe.setup_intent(&customer_id).await?;
    let payment_method = state.stripe.payment_methods(&customer_id).await?;
    let plan = current_plan(&pg, &user.email).await?;
    let rendered_html = state.templates.render(
        "account",
        &json!({
            "user": user,
            "flash": FlashMessage::from(flash.clone()),
            "stripe_pub_key": state.stripe_pub_key.to_string(),
            "client_secret": intent.client_secret.to_string(),
            "plan": plan,
            "payment_method": payment_method,
        }),
    )?;
    Ok((flash, Html(rendered_html)).into_response())
}

async fn current_plan(
    pg: &tokio_postgres::Client,
    email: &str,
) -> Result<Option<String>, web::Error> {
    let res = pg
        .query(
            "
            select name
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
        Ok(Some(row.get(0)))
    }
}

async fn setup_stripe(
    pg: &tokio_postgres::Client,
    stripe: &stripe::Client,
    email: &str,
) -> Result<String, web::Error> {
    let res = pg
        .query(
            "select stripe_id from accounts where owner_email = $1",
            &[&email],
        )
        .await?;
    if res.is_empty() {
        tracing::debug!("creating stripe customer for: {}", email);
        let customer = stripe.create_customer(email).await?;
        pg.execute(
            "insert into accounts (owner_email, stripe_id) values ($1, $2)",
            &[&email, &customer.id],
        )
        .await?;
        Ok(customer.id)
    } else {
        let strip_id: String = res.first().unwrap().get(0);
        tracing::debug!("stripe customer exists for {}", email);
        Ok(strip_id)
    }
}
