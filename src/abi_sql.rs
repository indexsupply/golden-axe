use alloy::{
    dyn_abi::{DynSolType, Specifier},
    hex,
    json_abi::{Event, EventParam},
};
use eyre::{eyre, Ok, Result};

pub fn create_view(event: &Event) -> Result<String> {
    let mut projections: Vec<String> = Vec::new();
    projections.push("block_num, tx_hash, log_idx, address".to_string());
    projections.push(indexed_sql(&event.inputs)?);
    projections.push(abi_sql(&event.inputs)?);

    Ok(format!(
        r#"
            create or replace view {} as
            select {}
            from logs
            where topics[1] = '\x{}'
        "#,
        event.name.to_lowercase(),
        &projections.join(","),
        hex::encode(event.selector()),
    ))
}

fn indexed_sql(inputs: &[EventParam]) -> Result<String> {
    Ok(inputs
        .iter()
        .filter(|inp| inp.indexed)
        .enumerate()
        .map(|(i, inp)| match inp.resolve()? {
            DynSolType::Address => Ok(format!(
                "abi_address(topics[{}]) as \"{}\"",
                i + 2,
                inp.name,
            )),
            DynSolType::FixedBytes(_) => Ok(format!("topics[{}] as {}", i + 2, inp.name)),
            _ => Err(eyre!("unable to generate sql for: {:?}", inp)),
        })
        .collect::<Result<Vec<String>>>()?
        .join(","))
}

fn abi_sql(inputs: &[EventParam]) -> Result<String> {
    Ok(inputs
        .iter()
        .filter(|i| !i.indexed)
        .enumerate()
        .map(|(i, input)| abi_type_sql(i, &input.name, &input.resolve()?))
        .collect::<Result<Vec<String>>>()?
        .join(","))
}

fn abi_type_sql(pos: usize, name: &str, t: &DynSolType) -> Result<String> {
    let alias = if name.is_empty() {
        "".to_string()
    } else {
        format!("as {}", name)
    };
    match t {
        DynSolType::Address => Ok(format!(
            "abi_address(abi_fixed_bytes(data, {}, 32)) {}",
            pos * 32,
            alias,
        )),
        DynSolType::Bool => Ok(String::new()),
        DynSolType::Bytes => Ok(format!(
            "abi_bytes(abi_dynamic(data, {})) {}",
            pos * 32,
            alias
        )),
        DynSolType::FixedBytes(_) => {
            Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias))
        }
        DynSolType::Array(arr) => match arr.as_ref() {
            DynSolType::FixedBytes(_) => Ok(format!(
                "abi_fixed_bytes_array(abi_dynamic(data, {}), 32) {}",
                pos * 32,
                alias
            )),
            DynSolType::Tuple(fields) => {
                let size = fields.iter().map(|f| f.minimum_words()).sum::<usize>() * 32;
                let key_names = (0..fields.iter().len()).map(|i| format!("'{}'", i));
                let values = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| abi_type_sql(i, "", field));
                let combined = key_names
                    .zip(values)
                    .try_fold(Vec::new(), |mut acc, (k, v)| {
                        v.map(|v| {
                            acc.extend([k, v]);
                            acc
                        })
                    })?;
                Ok(format!(
                    r#"(
                        select json_agg(json_build_object({}))
                        from unnest(abi_fixed_bytes_array(abi_dynamic(data, {}), {})) as data
                    ) {}"#,
                    combined.join(","),
                    pos * 32,
                    size,
                    alias
                ))
            }
            _ => Ok(String::new()),
        },
        DynSolType::Int(_) => Ok(String::new()),
        DynSolType::Uint(_) => Ok(format!(
            "abi_uint(abi_fixed_bytes(data, {}, 32)) {}",
            pos * 32,
            alias,
        )),
        _ => Err(eyre!("unable to generate sql for: {:?}", t)),
    }
}

const PG: &sqlparser::dialect::PostgreSqlDialect = &sqlparser::dialect::PostgreSqlDialect {};

pub fn fmt_sql(sql: &str) -> Result<String> {
    let ast = sqlparser::parser::Parser::parse_sql(PG, sql)?;
    Ok(sqlformat::format(
        &ast[0].to_string(),
        &sqlformat::QueryParams::None,
        sqlformat::FormatOptions::default(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_sql(event_sig: &str, want: &str) {
        let event: Event = event_sig
            .chars()
            .filter(|c| c.is_ascii())
            .collect::<String>()
            .replace('\n', "")
            .parse()
            .unwrap_or_else(|_| panic!("unable to parse {}", event_sig));
        let got = create_view(&event)
            .unwrap_or_else(|_| panic!("unable to create sql for {}", event_sig));
        let (got, want) = (
            fmt_sql(&got).unwrap_or_else(|_| panic!("unable to format got: {}", got)),
            fmt_sql(want).unwrap_or_else(|_| panic!("unable to format want: {}", want)),
        );
        if got.to_lowercase().ne(&want.to_lowercase()) {
            panic!("got:\n{}\n\nwant:\n{}\n", got, want);
        }
    }

    #[test]
    fn test_create_erc20() {
        check_sql(
            "Transfer(address indexed from, address indexed to, uint tokens)",
            r#"
                create or replace view transfer as
                    select
                        block_num,
                        tx_hash,
                        log_idx,
                        address,
                        abi_address(topics[2]) as "from",
                        abi_address(topics[3]) as "to",
                        abi_uint(abi_fixed_bytes(data, 0, 32)) as tokens
                    from logs
                    where topics[1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            "#,
        )
    }

    #[test]
    fn test_create_mud() {
        check_sql(
            "Store_SetRecord(bytes32 indexed tableId, bytes32[] keyTuple, bytes staticData, bytes32 encodedLengths, bytes dynamicData)",
            r#"
                create or replace view store_setrecord as
                    select
                        block_num,
                        tx_hash,
                        log_idx,
                        address,
                        topics[2] as tableid,
                        abi_fixed_bytes_array(abi_dynamic(data, 0), 32) as keytuple,
                        abi_bytes(abi_dynamic(data, 32)) as staticdata,
                        abi_fixed_bytes(data, 64, 32) as encodedlengths,
                        abi_bytes(abi_dynamic(data, 96)) as dynamicdata
                    from logs
                    where topics[1] = '\x8dbb3a9672eebfd3773e72dd9c102393436816d832c7ba9e1e1ac8fcadcac7a9'
            "#,
        )
    }

    #[test]
    fn test_seaport() {
        check_sql(
            "OrderFulfilled(bytes32 orderHash, address indexed offerer, address indexed zone, address recipient, (uint8, address, uint256, uint256)[] offer, (uint8, address, uint256, uint256, address)[] consideration)",
            r#"
                create or replace view orderfulfilled as
                select
                    block_num,
                    tx_hash,
                    log_idx,
                    address,
                    abi_address(topics[2]) as "offerer",
                    abi_address(topics[3]) as "zone",
                    abi_fixed_bytes(data, 0, 32) as orderhash,
                    abi_address(abi_fixed_bytes(data, 32, 32)) as recipient,
                    (
                        select json_agg(json_build_object(
                            '0', abi_uint(abi_fixed_bytes(data, 0, 32)),
                            '1', abi_address(abi_fixed_bytes(data, 32, 32)),
                            '2', abi_uint(abi_fixed_bytes(data, 64, 32)),
                            '3', abi_uint(abi_fixed_bytes(data, 96, 32))
                        )) from unnest(abi_fixed_bytes_array(abi_dynamic(data, 64), 128)) as data
                    ) as offer,
                    (
                        select json_agg(json_build_object(
                            '0', abi_uint(abi_fixed_bytes(data, 0, 32)),
                            '1', abi_address(abi_fixed_bytes(data, 32, 32)),
                            '2', abi_uint(abi_fixed_bytes(data, 64, 32)),
                            '3', abi_uint(abi_fixed_bytes(data, 96, 32)),
                            '4', abi_address(abi_fixed_bytes(data, 128, 32))
                        )) from unnest(abi_fixed_bytes_array(abi_dynamic(data, 96), 160)) as data
                    ) as consideration
                from logs
                where topics[1] = '\x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
            "#
        );
    }
}
