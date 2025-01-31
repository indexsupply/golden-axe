use be::abi;
use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[pg_extern(immutable, parallel_safe)]
fn abi2json(data: &[u8], desc: &str) -> pgrx::JsonB {
    let param = abi::Parameter::parse(desc).expect("unable to parse abi signature");
    let parsed = abi::to_json(data, &param).expect("unable to decode");
    pgrx::JsonB(parsed)
}
