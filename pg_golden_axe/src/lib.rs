use be::abi;
use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[pg_extern(immutable, parallel_safe)]
fn abi2json(data: &[u8], desc: &str) -> pgrx::JsonB {
    pgrx::JsonB(
        abi::Parameter::parse(desc)
            .expect("unable to parse abi signature")
            .to_json(data)
            .expect("decoding abi data"),
    )
}
