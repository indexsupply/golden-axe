use axum::extract::FromRef;
use axum_extra::extract::cookie::Key;
use axum_flash::IncomingFlashes;
use deadpool_postgres::Pool;
use serde::Serialize;

use crate::{daimo, postmark, query, stripe};

#[derive(Clone)]
pub struct State {
    pub be_url: String,
    pub fe_url: String,
    pub flash: axum_flash::Config,
    pub templates: handlebars::Handlebars<'static>,
    pub pool: Pool,
    pub key: Key,
    pub postmark: postmark::Client,
    pub stripe_pub_key: Option<String>,
    pub stripe: stripe::Client,
    pub examples: Vec<query::Query>,
    pub daimo: daimo::Client,
}

impl FromRef<State> for Key {
    fn from_ref(state: &State) -> Self {
        state.key.clone()
    }
}

impl FromRef<State> for axum_flash::Config {
    fn from_ref(state: &State) -> axum_flash::Config {
        state.flash.clone()
    }
}

#[derive(Serialize)]
pub struct FlashMessage {
    pub level: String,
    pub message: String,
}

impl FlashMessage {
    pub fn from(flashes: IncomingFlashes) -> Vec<Self> {
        flashes
            .into_iter()
            .map(|f| FlashMessage {
                level: format!("{:?}", f.0),
                message: f.1.to_string(),
            })
            .collect()
    }
}
