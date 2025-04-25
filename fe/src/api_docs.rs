use axum::{
    extract::{OriginalUri, State},
    response::Html,
};
use pulldown_cmark::{html, Options, Parser};
use serde_json::json;

use crate::{chains, web};

pub async fn index(
    State(state): State<web::State>,
    uri: OriginalUri,
) -> Result<Html<String>, shared::Error> {
    let path = uri.0.path();
    let template_name = if path == "/docs/v2" {
        "docs/index-v2.md"
    } else {
        "docs/index.md"
    };
    let pg = state.pool.get().await?;
    let chains = chains::list(&pg)
        .await?
        .into_iter()
        .filter(|c| c.enabled)
        .collect::<Vec<_>>();
    let index = state
        .templates
        .render(template_name, &json!({"chains": chains, }))?;
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
