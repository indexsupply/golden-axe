use be::abi;
use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[pg_extern]
fn abi2json(data: &[u8], desc: &str) -> pgrx::JsonB {
    let param = abi::parse(desc).expect("decoding abi signature");
    let parsed = abi::to_json(data, &param).expect("decoding abi");
    pgrx::JsonB(parsed)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
