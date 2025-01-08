use crate::Error;

pub fn unique_violations(err: tokio_postgres::Error, map: &[(&str, &str)]) -> Error {
    err.as_db_error()
        .filter(|e| e.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
        .and_then(|e| {
            map.iter()
                .find(|(c, _)| e.constraint().unwrap_or_default() == *c)
                .map(|(_, msg)| msg.to_string())
        })
        .map(Error::User)
        .unwrap_or_else(|| err.into())
}
