use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, FromRequestParts, State},
    response::{Html, IntoResponse, Redirect},
    Form,
};
use axum_extra::extract::{cookie::Cookie, SignedCookieJar};
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::{Duration, OffsetDateTime};

use crate::web::{self, FlashMessage};

#[derive(Serialize)]
pub struct User {
    pub email: String,
}

#[axum::async_trait]
impl FromRequestParts<web::State> for User {
    type Rejection = axum::response::Response;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &web::State,
    ) -> Result<Self, Self::Rejection> {
        let jar: SignedCookieJar = SignedCookieJar::from_request_parts(parts, state)
            .await
            .unwrap();
        match jar.get("email") {
            Some(cookie) => Ok(User {
                email: cookie.value().to_string(),
            }),
            None => Err(Redirect::temporary("/login").into_response()),
        }
    }
}

pub async fn try_login(
    State(state): State<web::State>,
    flash: axum_flash::IncomingFlashes,
) -> Result<impl IntoResponse, shared::Error> {
    Ok(Html(state.templates.render(
        "login.html",
        &json!({
            "flash": FlashMessage::from(flash.clone()),
        }),
    )?))
}

#[derive(Deserialize)]
pub struct EmailLoginRequest {
    email: String,

    #[serde(rename = "username")]
    honeypot: Option<String>,
}

pub async fn email_login_link(
    flash: axum_flash::Flash,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<web::State>,
    Form(req): Form<EmailLoginRequest>,
) -> Result<impl IntoResponse, shared::Error> {
    if req
        .honeypot
        .as_ref()
        .map(|h| !h.is_empty())
        .unwrap_or(false)
    {
        tracing::info!(
            "gotch ya! email: {} username: {}",
            req.email,
            req.honeypot.unwrap()
        );
        return Ok("Thank you".into_response());
    }
    let mut secret = vec![0u8; 32];
    getrandom::getrandom(&mut secret).wrap_err("unable to generate secret")?;
    let mut pg = state.pool.get().await?;
    let pgtx = pg.transaction().await?;
    pgtx.execute(
        "update login_links set invalidated_at = now() where email = $1",
        &[&req.email],
    )
    .await?;
    pgtx.execute(
        "insert into login_links(email, secret, created_by) values ($1, $2, $3)",
        &[&req.email, &secret, &addr.ip()],
    )
    .await?;
    pgtx.commit().await?;
    match send_email_login(state, &req.email, secret).await {
        Ok(_) => {
            let flash = flash.success("Please check your email to log in.");
            Ok((flash, Redirect::to("/")).into_response())
        }
        Err(e) => {
            tracing::error!("sending email: {}", e);
            let flash = flash.error("Error sending email. Please try again.");
            Ok((flash, Redirect::to("/login")).into_response())
        }
    }
}

async fn send_email_login(state: web::State, to: &str, secret: Vec<u8>) -> Result<()> {
    let body = format!(
        "Hello,\n\nHere is your one-time log in link: {}/email-login-link?secret={}\n\nIf you have any issues logging in, reply to this email to get help.\n\nRegards,\nIndex Supply",
        state.fe_url,
        hex::encode(secret),
    );
    state
        .postmark
        .send("login@indexsupply.net", to, "One-Time Log In Link", &body)
        .await
}

#[derive(Deserialize)]
pub struct LoginRequest {
    #[serde(with = "hex")]
    secret: Vec<u8>,
}

pub async fn login(
    State(state): State<web::State>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Form(req): Form<LoginRequest>,
) -> Result<impl IntoResponse, shared::Error> {
    let pg = state.pool.get().await?;
    const Q: &str = r#"
        update login_links
        set completed_at = now(), completed_by = $1
        where secret = $2
        and invalidated_at is null
        and completed_at is null
        returning email
    "#;
    let res = pg.query(Q, &[&addr.ip(), &req.secret]).await?;
    if res.is_empty() {
        tracing::info!(
            "failed login attempt by: {} secret: {}",
            addr.ip(),
            hex::encode(&req.secret)
        );
        let flash = vec![FlashMessage {
            level: "Error".to_string(),
            message: "please request new login link".to_string(),
        }];
        let resp = Html(
            state
                .templates
                .render("index.html", &json!({"flash": flash}))?,
        );
        Ok(resp.into_response())
    } else {
        let res = res.first().expect("no rows found");
        let email: String = res.get(0);
        let one_week = OffsetDateTime::now_utc() + Duration::weeks(1);
        let cookie = Cookie::build(("email", email))
            .expires(one_week)
            .http_only(true)
            .same_site(axum_extra::extract::cookie::SameSite::Lax)
            .build();
        let jar = SignedCookieJar::new(state.key).add(cookie);
        Ok((jar, Redirect::to("/account")).into_response())
    }
}

pub async fn logout(State(state): State<web::State>) -> impl IntoResponse {
    let cookie = Cookie::build(("email", "")).removal().build();
    let jar = SignedCookieJar::new(state.key).add(cookie);
    (jar, Redirect::to("/"))
}
