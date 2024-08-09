use axum::response::IntoResponse;
use axum_extra::extract::SignedCookieJar;
use maud::html;

use crate::session;

pub async fn index(flash: axum_flash::IncomingFlashes, jar: SignedCookieJar) -> impl IntoResponse {
    let resp = html! {
        @for (_level, message) in &flash {
            p {(message)}
        }
        @match session::User::from_jar(jar) {
            Some(u) => span {"hi: " (u.email)},
            None => span { "please log in" }
        }
    };
    (flash, resp).into_response()
}
