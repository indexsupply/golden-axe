use alloy::{
    dyn_abi::{DynSolType, Specifier},
    hex,
    json_abi::{Event, EventParam},
};
use eyre::{eyre, Context, Result};

use crate::{api, sql_validate};

pub fn view(event: &Event) -> Result<String> {
    let mut projections: Vec<String> = Vec::new();
    projections.push("block_num, tx_hash, log_idx, address".to_string());
    projections.push(topics_sql_all(&event.inputs)?);
    projections.push(abi_sql_all(&event.inputs)?);

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

pub fn query(
    user_query: &str,
    event_sigs: Vec<&str>,
    from: Option<u64>,
) -> Result<String, api::Error> {
    tracing::info!(
        "event_sigs: {} user_query: {}",
        event_sigs
            .iter()
            .map(|s| s.trim())
            .collect::<Vec<&str>>()
            .join(","),
        user_query
            .trim()
            .replace(['\n', '\t'], " ")
            .split_whitespace()
            .collect::<Vec<&str>>()
            .join(" "),
    );
    let res = sql_validate::validate(user_query, event_sigs)?;
    let query: Vec<String> = vec![
        "with".to_string(),
        limit_block_range(from),
        res.selections
            .iter()
            .map(selection_cte_sql)
            .collect::<Result<Vec<_>, _>>()?
            .join(","),
        res.new_query.to_string(),
    ];
    tracing::info!("query: {}", query.join(" "));
    Ok(query.join(" "))
}

fn limit_block_range(from: Option<u64>) -> String {
    match from {
        Some(n) => format!("logs as (select * from logs where block_num >= {}),", n),
        None => String::new(),
    }
}

fn selection_cte_sql(selection: &sql_validate::Selection) -> Result<String, api::Error> {
    let mut res: Vec<String> = Vec::new();
    res.push(format!("{} as (", selection.user_event_name));
    res.push("select".to_string());
    let mut select_list = Vec::new();
    selection.fields.iter().for_each(|f| {
        if sql_validate::METADATA.contains(&f.as_str()) {
            select_list.push(f.to_string());
        }
    });
    let indexed_inputs = selection
        .event
        .inputs
        .iter()
        .filter(|inp| inp.indexed)
        .enumerate();
    for (i, inp) in indexed_inputs {
        if selection.selected_field(&inp.name) {
            let t = inp.resolve().wrap_err("unable to resolve input")?;
            let name = selection.quoted_field_name(&inp.name)?;
            select_list.push(topic_sql(i, &name, &t)?)
        }
    }
    let abi_inputs = selection
        .event
        .inputs
        .iter()
        .filter(|inp| !inp.indexed)
        .enumerate();
    for (i, inp) in abi_inputs {
        if selection.selected_field(&inp.name) {
            let t = inp.resolve().wrap_err("unable to resolve input")?;
            let name = selection.quoted_field_name(&inp.name)?;
            select_list.push(abi_sql(i, &name, &t)?)
        }
    }
    res.push(select_list.join(","));
    res.push(format!(
        r#"from logs where topics[1] = '\x{}'"#,
        hex::encode(selection.event.selector())
    ));
    res.push(")".to_string());
    Ok(res.join(" "))
}

fn topics_sql_all(inputs: &[EventParam]) -> Result<String> {
    Ok(inputs
        .iter()
        .filter(|inp| inp.indexed)
        .enumerate()
        .map(|(i, input)| topic_sql(i, &format!("\"{}\"", &input.name), &input.resolve()?))
        .collect::<Result<Vec<String>>>()?
        .join(","))
}

fn topic_sql(pos: usize, name: &str, _t: &DynSolType) -> Result<String> {
    // postgres arrays are 1-indexed and the first element is
    // the event signature hash
    let pos = pos + 2;
    Ok(format!("topics[{}] as {}", pos, name))
}

fn abi_sql_all(inputs: &[EventParam]) -> Result<String> {
    Ok(inputs
        .iter()
        .filter(|i| !i.indexed)
        .enumerate()
        .map(|(i, input)| abi_sql(i, &format!("\"{}\"", &input.name), &input.resolve()?))
        .collect::<Result<Vec<String>>>()?
        .join(","))
}

fn abi_sql(pos: usize, name: &str, t: &DynSolType) -> Result<String> {
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
        DynSolType::Bool => todo!(),
        DynSolType::Bytes => Ok(format!(
            "abi_bytes(abi_dynamic(data, {})) {}",
            pos * 32,
            alias
        )),
        DynSolType::String => Ok(format!(
            r#"convert_from(rtrim(abi_bytes(abi_dynamic(data, {})), '\x00'), 'UTF8') {}"#,
            pos * 32,
            alias
        )),
        DynSolType::FixedBytes(_) => {
            Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias))
        }
        DynSolType::Int(_) => Ok(format!(
            "abi_int(abi_fixed_bytes(data, {}, 32)) {}",
            pos * 32,
            alias,
        )),
        DynSolType::Uint(_) => Ok(format!(
            "abi_uint(abi_fixed_bytes(data, {}, 32)) {}",
            pos * 32,
            alias,
        )),
        DynSolType::Array(arr) => match arr.as_ref() {
            DynSolType::FixedBytes(_) => Ok(format!(
                "abi_fixed_bytes_array(abi_dynamic(data, {}), 32) {}",
                pos * 32,
                alias
            )),
            DynSolType::Uint(_) => Ok(format!(
                "abi_uint_array(abi_dynamic(data, {})) {}",
                pos * 32,
                alias,
            )),
            DynSolType::Int(_) => Ok(format!(
                "abi_int_array(abi_dynamic(data, {})) {}",
                pos * 32,
                alias,
            )),
            DynSolType::Tuple(fields) => {
                let size = fields.iter().map(|f| f.minimum_words()).sum::<usize>() * 32;
                let key_names = (0..fields.iter().len()).map(|i| format!("'{}'", i));
                let values = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| abi_sql(i, "", field));
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
            _ => todo!(),
        },
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

    fn check_sql(event_sigs: Vec<&str>, user_query: &str, want: &str) {
        let got = query(user_query, event_sigs, None)
            .unwrap_or_else(|e| panic!("unable to create sql for {:?} {:?}", user_query, e));
        let (got, want) = (
            fmt_sql(&got).unwrap_or_else(|_| panic!("unable to format got: {}", got)),
            fmt_sql(want).unwrap_or_else(|_| panic!("unable to format want: {}", want)),
        );
        if got.to_lowercase().ne(&want.to_lowercase()) {
            panic!("got:\n{}\n\nwant:\n{}\n", got, want);
        }
    }

    #[test]
    fn test_rewrite_topics() {
        check_sql(
            vec!["Transfer(address indexed from, address indexed to, uint indexed tokens)"],
            r#"
                select tokens
                from transfer
                where "from" = 0x00000000000000000000000000000000deadbeef
                and tokens > 1
            "#,
            r#"
                with transfer as (
                    select
                        topics[2] as "from",
                        topics[4] as tokens
                    from logs
                    where topics [1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                )
                select abi_uint(tokens) as tokens
                from transfer
                where "from" = '\x00000000000000000000000000000000000000000000000000000000deadbeef'
                and tokens > '\x0000000000000000000000000000000000000000000000000000000000000001'
            "#,
        );
    }

    #[test]
    fn test_erc20_sql() {
        check_sql(
            vec!["\r\nTransfer(address indexed from, address indexed to, uint tokens)\r\n"],
            r#"select "from", "to", tokens from transfer"#,
            r#"
                with transfer as (
                    select
                        topics[2] as "from",
                        topics[3] as "to",
                        abi_uint(abi_fixed_bytes(data, 0, 32)) AS tokens
                    from logs
                    where topics [1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                )
                select
                    abi_address("from") as "from",
                    abi_address("to") as "to",
                    tokens
                from transfer
            "#,
        );
    }

    #[test]
    fn test_joins() {
        check_sql(
            vec!["Foo(uint a, uint b)", "Bar(uint a, uint b)"],
            r#"select foo.b, bar.b from foo, bar where foo.a = bar.a"#,
            r#"
                with
                bar as (
                    select
                        abi_uint(abi_fixed_bytes(data, 0, 32)) as a,
                        abi_uint(abi_fixed_bytes(data, 32, 32)) as b
                    from logs
                    where topics [1] = '\xde24c8e88b6d926d4bd258eddfb15ef86337654619dec5f604bbdd9d9bc188ca'
                ),
                foo as (
                    select
                        abi_uint(abi_fixed_bytes(data, 0, 32)) as a,
                        abi_uint(abi_fixed_bytes(data, 32, 32)) as b
                    from logs
                    where topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select foo.b, bar.b
                from foo, bar
                where foo.a = bar.a
            "#,
        );
    }

    #[test]
    fn test_joins_with_unselected() {
        check_sql(
            vec!["Foo(uint a, uint b)", "Bar(uint a, uint b)"],
            r#"select foo.b from foo"#,
            r#"
                with foo as (
                    select abi_uint(abi_fixed_bytes(data, 32, 32)) as b
                    from logs
                    where topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select foo.b from foo
            "#,
        );
    }

    fn check_view(event_sig: &str, want: &str) {
        let event: Event = event_sig
            .chars()
            .filter(|c| c.is_ascii())
            .collect::<String>()
            .replace('\n', "")
            .parse()
            .unwrap_or_else(|_| panic!("unable to parse {}", event_sig));
        let got = view(&event).unwrap_or_else(|_| panic!("unable to create sql for {}", event_sig));
        let (got, want) = (
            fmt_sql(&got).unwrap_or_else(|_| panic!("unable to format got: {}", got)),
            fmt_sql(want).unwrap_or_else(|_| panic!("unable to format want: {}", want)),
        );
        if got.to_lowercase().ne(&want.to_lowercase()) {
            panic!("got:\n{}\n\nwant:\n{}\n", got, want);
        }
    }

    #[test]
    fn test_erc20_view() {
        check_view(
            "Transfer(address indexed from, address indexed to, uint tokens)",
            r#"
                create or replace view transfer as
                    select
                        block_num,
                        tx_hash,
                        log_idx,
                        address,
                        topics[2] as "from",
                        topics[3] as "to",
                        abi_uint(abi_fixed_bytes(data, 0, 32)) as "tokens"
                    from logs
                    where topics[1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            "#,
        )
    }

    #[test]
    fn test_mud_view() {
        check_view(
            "Store_SetRecord(bytes32 indexed tableId, bytes32[] keyTuple, bytes staticData, bytes32 encodedLengths, bytes dynamicData)",
            r#"
                create or replace view store_setrecord as
                    select
                        block_num,
                        tx_hash,
                        log_idx,
                        address,
                        topics[2] as "tableid",
                        abi_fixed_bytes_array(abi_dynamic(data, 0), 32) as "keytuple",
                        abi_bytes(abi_dynamic(data, 32)) as "staticdata",
                        abi_fixed_bytes(data, 64, 32) as "encodedlengths",
                        abi_bytes(abi_dynamic(data, 96)) as "dynamicdata"
                    from logs
                    where topics[1] = '\x8dbb3a9672eebfd3773e72dd9c102393436816d832c7ba9e1e1ac8fcadcac7a9'
            "#,
        )
    }

    #[test]
    fn test_seaport_view() {
        check_view(
            "OrderFulfilled(bytes32 orderHash, address indexed offerer, address indexed zone, address recipient, (uint8, address, uint256, uint256)[] offer, (uint8, address, uint256, uint256, address)[] consideration)",
            r#"
                create or replace view orderfulfilled as
                select
                    block_num,
                    tx_hash,
                    log_idx,
                    address,
                    topics[2] as "offerer",
                    topics[3] as "zone",
                    abi_fixed_bytes(data, 0, 32) as "orderhash",
                    abi_address(abi_fixed_bytes(data, 32, 32)) as "recipient",
                    (
                        select json_agg(json_build_object(
                            '0', abi_uint(abi_fixed_bytes(data, 0, 32)),
                            '1', abi_address(abi_fixed_bytes(data, 32, 32)),
                            '2', abi_uint(abi_fixed_bytes(data, 64, 32)),
                            '3', abi_uint(abi_fixed_bytes(data, 96, 32))
                        )) from unnest(abi_fixed_bytes_array(abi_dynamic(data, 64), 128)) as data
                    ) as "offer",
                    (
                        select json_agg(json_build_object(
                            '0', abi_uint(abi_fixed_bytes(data, 0, 32)),
                            '1', abi_address(abi_fixed_bytes(data, 32, 32)),
                            '2', abi_uint(abi_fixed_bytes(data, 64, 32)),
                            '3', abi_uint(abi_fixed_bytes(data, 96, 32)),
                            '4', abi_address(abi_fixed_bytes(data, 128, 32))
                        )) from unnest(abi_fixed_bytes_array(abi_dynamic(data, 96), 160)) as data
                    ) as "consideration"
                from logs
                where topics[1] = '\x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
            "#
        );
    }
}
