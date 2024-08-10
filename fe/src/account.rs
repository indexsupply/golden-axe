use axum::{extract::State, response::IntoResponse};
use axum_extra::extract::SignedCookieJar;
use deadpool_postgres::Pool;
use eyre::Result;
use maud::html;

use crate::{session, stripe, web};

/*

    Index / Logged In

    - API Keys
    - Plan Info
    - Read Docs
    - Support

    Inded / Logged Out

    - Log in
    - Read Docs
    - Support

*/
pub async fn index(
    State(state): State<web::State>,
    flash: axum_flash::IncomingFlashes,
    jar: SignedCookieJar,
) -> Result<impl IntoResponse, web::Error> {
    let user = session::User::from_jar(jar);
    let stripe_id = if let Some(u) = &user {
        let id = setup_stripe(state.pool, state.stripe, &u.email).await?;
        Some(id)
    } else {
        None
    };
    let resp = html! {
        @for (_level, message) in &flash {
            p {(message)}
        }
        @match user {
            Some(u) => span {"hi: " (u.email)},
            None => span { "please log in" }
        }
        @match stripe_id {
            Some(id) => p {"stripe: " (id)},
            None => p {"missing stripe customer id"}
        }
    };
    Ok((flash, resp).into_response())
}

async fn setup_stripe(pg: Pool, stripe: stripe::Client, email: &str) -> Result<String, web::Error> {
    let res = pg
        .get()
        .await?
        .query(
            r#"select stripe_id from accounts where owner_email = $1"#,
            &[&email],
        )
        .await?;
    if res.is_empty() {
        tracing::debug!("creating stripe customer for: {}", email);
        let customer = stripe.create_customer(email).await?;
        pg.get()
            .await?
            .execute(
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
