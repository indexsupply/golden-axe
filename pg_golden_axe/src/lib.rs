use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[pg_extern]
fn abi2json(_data: &[u8], _desc: &str) -> pgrx::JsonB {
    pgrx::JsonB(serde_json::Value::String(String::from("golden axe!")))
}
