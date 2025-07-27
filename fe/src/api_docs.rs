use axum::{extract::State, response::Html};
use pulldown_cmark::{html, Options, Parser};
use serde_json::json;

use crate::{chains, web};

pub async fn index(State(state): State<web::State>) -> Result<Html<String>, shared::Error> {
    let pg = state.pool.get().await?;
    let chains = chains::list(&pg)
        .await?
        .into_iter()
        .filter(|c| c.enabled)
        .map(|mut c| {
            c.start_block = Some(c.start_block.unwrap_or(0));
            c
        })
        .collect::<Vec<_>>();
    let index = state
        .templates
        .render("docs/index.md", &json!({"chains": chains, }))?;
    let mut options = Options::empty();
    options.insert(Options::ENABLE_GFM);
    options.insert(Options::ENABLE_HEADING_ATTRIBUTES);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_FOOTNOTES);
    let parser = Parser::new_ext(&index, options);
    let mut body = String::new();
    html::push_html(&mut body, parser);

    Ok(Html(
        state
            .templates
            .render("docs.html", &json!({"body": body }))?,
    ))
}
