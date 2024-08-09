use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, State},
    response::{IntoResponse, Redirect},
    Form,
};
use axum_extra::extract::{cookie::Cookie, SignedCookieJar};
use eyre::{Context, Result};
use maud::html;
use serde::Deserialize;
use time::{Duration, OffsetDateTime};

use crate::web;

pub struct User {
    pub email: String,
}

impl User {
    pub fn from_jar(jar: SignedCookieJar) -> Option<User> {
        jar.get("email").map(|c| User {
            email: c.to_string(),
        })
    }
}

#[derive(Deserialize)]
pub struct EmailLoginRequest {
    email: String,
}

pub async fn email_login_link(
    flash: axum_flash::Flash,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(config): State<web::Config>,
    Form(req): Form<EmailLoginRequest>,
) -> Result<impl IntoResponse, web::Error> {
    let mut secret = vec![0u8; 32];
    getrandom::getrandom(&mut secret).wrap_err("unable to generate secret")?;
    let mut pg = config.pool.get().await?;
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
    config.sendgrid.send_email_login(&req.email, secret).await?;
    let flash = flash.success("Please check your email to log in.");
    Ok((flash, Redirect::to("/")))
}

#[derive(Deserialize)]
pub struct LoginRequest {
    #[serde(with = "hex")]
    secret: Vec<u8>,
}

pub async fn login(
    flash: axum_flash::Flash,
    State(config): State<web::Config>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Form(req): Form<LoginRequest>,
) -> Result<impl IntoResponse, web::Error> {
    const Q: &str = r#"
        update login_links
        set completed_at = now(), completed_by = $1
        where secret = $2
        and invalidated_at is null
        and completed_at is null
        returning email
    "#;
    let res = config
        .pool
        .get()
        .await?
        .query(Q, &[&addr.ip(), &req.secret])
        .await?;
    if res.is_empty() {
        let flash = flash.error("Please request a log in link.");
        Ok((flash, Redirect::to("/")).into_response())
    } else {
        let res = res.first().expect("no rows found");
        let email: String = res.get(0);
        let one_week = OffsetDateTime::now_utc() + Duration::weeks(1);
        let cookie = Cookie::build(("email", email)).expires(one_week).build();
        let jar = SignedCookieJar::new(config.key).add(cookie);
        Ok((jar, Redirect::to("/")).into_response())
    }
}

pub async fn logout(State(config): State<web::Config>) -> impl IntoResponse {
    let cookie = Cookie::build(("email", "")).removal().build();
    let jar = SignedCookieJar::new(config.key).add(cookie);
    (jar, Redirect::to("/"))
}

pub async fn try_login() -> impl IntoResponse {
    html! {
        body {
            form method="post" action="/email-login-link" {
                label for="email" { "Email: " }
                input type="email" id="email" name="email" required {}
                button type="submit" { "Submit" }
            }
        }
    }
}
