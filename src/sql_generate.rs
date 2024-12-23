use alloy::{
    dyn_abi::{DynSolType, Specifier},
    hex,
};
use eyre::{eyre, Context, Result};
use itertools::Itertools;

use crate::{api, user_query};

pub struct Query {
    pub event_sigs: Vec<String>,
    pub user_query: String,
    pub rewritten_query: String,
    pub generated_query: String,
}

pub fn query(
    chain: api::Chain,
    from: Option<u64>,
    user_query: &str,
    event_sigs: Vec<&str>,
) -> Result<Query, api::Error> {
    let res = user_query::process(user_query, &event_sigs)?;
    let query = [
        "with".to_string(),
        res.relations
            .iter()
            .map(|sel| relation_cte_sql(chain, from, sel))
            .collect::<Result<Vec<_>, _>>()?
            .join(","),
        res.new_query.to_string(),
    ]
    .join(" ");
    Ok(Query {
        event_sigs: event_sigs.into_iter().map(|s| s.to_string()).collect(),
        user_query: user_query.to_string(),
        rewritten_query: res.new_query,
        generated_query: query,
    })
}

fn relation_cte_sql(
    chain: api::Chain,
    from: Option<u64>,
    rel: &user_query::Relation,
) -> Result<String, api::Error> {
    let mut res: Vec<String> = Vec::new();
    res.push(format!("{} as not materialized (", rel.table_name));
    res.push("select".to_string());
    let mut select_list = Vec::new();
    rel.fields.iter().sorted().for_each(|f| {
        if user_query::METADATA.contains(&f.as_str()) {
            select_list.push(f.to_string());
        }
    });
    let indexed_inputs = rel
        .event
        .iter()
        .flat_map(|event| event.inputs.iter().filter(|inp| inp.indexed).enumerate());
    for (i, inp) in indexed_inputs {
        if rel.selected_field(&inp.name) {
            let t = inp.resolve().wrap_err("unable to resolve input")?;
            let name = rel.quoted_field_name(&inp.name)?;
            select_list.push(topic_sql(i, &name, &t)?)
        }
    }
    let abi_inputs = rel
        .event
        .iter()
        .flat_map(|event| event.inputs.iter().filter(|inp| !inp.indexed).enumerate());
    for (i, inp) in abi_inputs {
        if rel.selected_field(&inp.name) {
            let t = inp.resolve().wrap_err("unable to resolve input")?;
            let name = rel.quoted_field_name(&inp.name)?;
            select_list.push(abi_sql(i, &name, &t)?)
        }
    }
    res.push(select_list.join(","));
    res.push(format!("from logs where chain = {}", chain,));
    if let Some(topic) = rel.event.as_ref().map(|e| e.selector()) {
        res.push(format!(r#"and topics[1] = '\x{}'"#, hex::encode(topic)))
    }
    if let Some(n) = from {
        res.push(format!("and block_num >= {}", n))
    }
    res.push(")".to_string());
    Ok(res.join(" "))
}

fn topic_sql(pos: usize, name: &str, _t: &DynSolType) -> Result<String> {
    // postgres arrays are 1-indexed and the first element is
    // the event signature hash
    let pos = pos + 2;
    Ok(format!("topics[{}] as {}", pos, name))
}

fn abi_sql(pos: usize, name: &str, t: &DynSolType) -> Result<String> {
    let alias = if name.is_empty() {
        "".to_string()
    } else {
        format!("as {}", name)
    };
    match t {
        DynSolType::Address => Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias,)),
        DynSolType::Bool => Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias)),
        DynSolType::Bytes => Ok(format!(
            "abi_bytes(abi_dynamic(data, {})) {}",
            pos * 32,
            alias
        )),
        DynSolType::String => Ok(format!(
            r#"abi_bytes(abi_dynamic(data, {})) {}"#,
            pos * 32,
            alias
        )),
        DynSolType::FixedBytes(_) => {
            Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias))
        }
        DynSolType::Int(_) => Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias,)),
        DynSolType::Uint(_) => Ok(format!("abi_fixed_bytes(data, {}, 32) {}", pos * 32, alias,)),
        DynSolType::Array(arr) => match arr.as_ref() {
            DynSolType::FixedBytes(_) => Ok(format!("abi_dynamic(data, {}) {}", pos * 32, alias)),
            DynSolType::Uint(_) => Ok(format!("abi_dynamic(data, {}) {}", pos * 32, alias,)),
            DynSolType::Int(_) => Ok(format!("abi_dynamic(data, {}) {}", pos * 32, alias,)),
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
            _ => Err(eyre!("unable to generate sql for array of: {:?}", t)),
        },
        _ => Err(eyre!("unable to generate sql for: {:?}", t)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg;

    const PG: &sqlparser::dialect::PostgreSqlDialect = &sqlparser::dialect::PostgreSqlDialect {};

    pub fn fmt_sql(sql: &str) -> Result<String> {
        let ast = sqlparser::parser::Parser::parse_sql(PG, sql)?;
        Ok(sqlformat::format(
            &ast[0].to_string(),
            &sqlformat::QueryParams::None,
            sqlformat::FormatOptions::default(),
        ))
    }

    async fn check_sql(event_sigs: Vec<&str>, user_query: &str, want: &str) {
        let got = query(1.into(), None, user_query, event_sigs)
            .unwrap_or_else(|e| panic!("unable to create sql for:\n{} error: {:?}", user_query, e))
            .generated_query;
        let (got, want) = (
            fmt_sql(&got).unwrap_or_else(|_| panic!("unable to format got: {}", got)),
            fmt_sql(want).unwrap_or_else(|_| panic!("unable to format want: {}", want)),
        );
        if got.to_lowercase().ne(&want.to_lowercase()) {
            panic!("got:\n{}\n\nwant:\n{}\n", got, want);
        }
        let (_pg_server, pool) = pg::test_utils::test_pg().await;
        let pg = pool.get().await.expect("getting pg from test pool");
        pg.query(&got, &[]).await.expect("issue with query");
    }

    #[tokio::test]
    async fn test_logs_table() {
        check_sql(
            vec![],
            r#"select block_num, data, topics from logs where topics[1] = 0xface"#,
            r#"
                with logs as not materialized (
                    select block_num, data, topics
                    from logs
                    where chain = 1
                )
                select block_num, data, topics
                from logs
                where topics[1] = '\xface'
            "#,
        )
        .await;
    }

    #[tokio::test]
    async fn test_nested_expressions() {
        check_sql(
            vec!["Foo(uint a, uint b)"],
            r#"
                select a
                from foo
                where a = 1
                and (b = 1 OR b = 0)
            "#,
            r#"
                with foo as not materialized (
                    select
                        abi_fixed_bytes(data, 0, 32) as a,
                        abi_fixed_bytes(data, 32, 32) as b
                    from logs
                    where chain = 1
                    and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select abi_uint(a) as a
                from foo
                where a = '\x0000000000000000000000000000000000000000000000000000000000000001'
                and (
                    b = '\x0000000000000000000000000000000000000000000000000000000000000001'
                    or b = '\x0000000000000000000000000000000000000000000000000000000000000000'
                )
            "#,
        )
        .await;
    }

    #[tokio::test]
    async fn test_abi_types() {
        check_sql(
            vec!["Foo(string a, bytes16 b, bytes c, int256 d, int256[] e, string[] f, bool g)"],
            r#"
                select a, b, c, d, e, g
                from foo
                where g = true
            "#,
            r#"
                with foo as not materialized (
                    select
                        abi_bytes(abi_dynamic(data, 0)) AS a,
                        abi_fixed_bytes(data, 32, 32) AS b,
                        abi_bytes(abi_dynamic(data, 64)) AS c,
                        abi_fixed_bytes(data, 96, 32) AS d,
                        abi_dynamic(data, 128) AS e,
                        abi_fixed_bytes(data, 192, 32) AS g
                    from logs
                    where chain = 1
                    and topics [1] = '\xfd2ebf78a81dba87ac294ee45944682ec394bb42128c245fca0eeab2d699c315'
                )
                select
                    abi_string(a) as a,
                    b,
                    c,
                    abi_int(d) AS d,
                    abi_int_array(e) AS e,
                    abi_bool(g) AS g
                from foo
                where g = '\x0000000000000000000000000000000000000000000000000000000000000001'

            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_variable_casing() {
        check_sql(
            vec!["Foo(uint indexed aAA, uint indexed b)"],
            r#"
                select "aAA", "b"
                from foo
            "#,
            r#"
                with foo as not materialized (
                    select
                        topics [2] as "aAA",
                        topics [3] as "b"
                    from logs
                    where chain = 1
                    and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select
                    abi_uint("aAA") as "aAA",
                    abi_uint("b") as "b"
                from foo
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_alias_group_by() {
        check_sql(
            vec!["Foo(uint indexed a, uint indexed b)"],
            r#"
                select
                    a as alpha,
                    count(b) as beta
                from foo
                group by alpha
                order by beta desc
            "#,
            r#"
                with foo as not materialized (
                    select
                        topics [2] as a,
                        topics [3] as b
                    from logs
                    where chain = 1
                    and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select
                    abi_uint(a) as alpha,
                    count(abi_uint(b)) as beta
                from foo
                group by alpha
                order by beta desc
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_topics() {
        check_sql(
            vec!["Transfer(address indexed from, address indexed to, uint indexed tokens)"],
            r#"
                select tokens
                from transfer
                where "from" = 0x00000000000000000000000000000000deadbeef
                and tokens > 1
            "#,
            r#"
                with transfer as not materialized (
                    select
                        topics[2] as "from",
                        topics[4] as tokens
                    from logs
                    where chain = 1
                    and topics [1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                )
                select abi_uint(tokens) as tokens
                from transfer
                where "from" = '\x00000000000000000000000000000000000000000000000000000000deadbeef'
                and tokens > '\x0000000000000000000000000000000000000000000000000000000000000001'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_topics_and_data() {
        check_sql(
            vec!["Transfer(address indexed from, address indexed to, uint tokens)"],
            r#"
                select tokens
                from transfer
                where "from" = 0x00000000000000000000000000000000deadbeef
                and tokens > 1
            "#,
            r#"
                with transfer as not materialized (
                    select
                        topics[2] as "from",
                        abi_fixed_bytes(data, 0, 32) AS tokens
                    from logs
                    where chain = 1
                    and topics [1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                )
                select abi_uint(tokens) as tokens
                from transfer
                where "from" = '\x00000000000000000000000000000000000000000000000000000000deadbeef'
                and tokens > '\x0000000000000000000000000000000000000000000000000000000000000001'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_literal_string() {
        check_sql(
            vec!["Foo(string bar)"],
            r#"select bar from foo where bar = 'baz'"#,
            r#"
                with foo as not materialized (
                    select abi_bytes(abi_dynamic(data, 0)) as bar
                    from logs
                    where chain = 1
                    and topics [1] = '\x9f0b7f1630bdb7d474466e2dfef0fb9dff65f7a50eec83935b68f77d0808f08a'
                )
                select abi_string(bar) as bar
                from foo
                where bar = '\x62617a'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_literal_address() {
        check_sql(
            vec!["Transfer(address indexed from, address indexed to, uint tokens)"],
            r#"
                select tokens
                from transfer
                where address = 0x00000000000000000000000000000000deadbeef
                and tx_hash = 0xface000000000000000000000000000000000000000000000000000000000000
            "#,
            r#"
                with transfer as not materialized (
                    select
                        address,
                        tx_hash,
                        abi_fixed_bytes(data, 0, 32) AS tokens
                    from logs
                    where chain = 1
                    and topics [1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                )
                select abi_uint(tokens) as tokens
                from transfer
                where address = '\x00000000000000000000000000000000deadbeef'
                and tx_hash = '\xface000000000000000000000000000000000000000000000000000000000000'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_select_function_args() {
        check_sql(
            vec!["Foo(address indexed a, uint b)"],
            r#"
                select sum(b)
                from foo
                where a = 0x00000000000000000000000000000000deadbeef
            "#,
            r#"
                with foo as not materialized (
                    select
                        topics[2] as a,
                        abi_fixed_bytes(data, 0, 32) AS b
                    from logs
                    where chain = 1
                    and topics [1] = '\xf31ba491e89b510fc888156ac880594d589edc875cfc250c79628ea36dd022ed'
                )
                select sum(abi_uint(b))
                from foo
                where a = '\x00000000000000000000000000000000000000000000000000000000deadbeef'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_bool() {
        check_sql(
            vec!["Foo(uint indexed a, bool b)"],
            r#"
                select b
                from foo
                where a = 0x00000000000000000000000000000000deadbeef
            "#,
            r#"
                with foo as not materialized (
                    select
                        topics[2] as a,
                        abi_fixed_bytes(data, 0, 32) AS b
                    from logs
                    where chain = 1
                    and topics [1] = '\x79c52e97493a8f32348c3cf1ebfe4a8dfaeb083ca12cddd87b5d9f7c00d3ccaa'
                )
                select
                    abi_bool(b) AS b
                from foo
                where a = '\x00000000000000000000000000000000000000000000000000000000deadbeef'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_arrays() {
        check_sql(
            vec!["Foo(uint indexed a, uint[] b, int256[] c)"],
            r#"
                select b, c
                from foo
                where a = 0x00000000000000000000000000000000deadbeef
            "#,
            r#"
                with foo as not materialized (
                    select
                        topics[2] as a,
                        abi_dynamic(data, 0) AS b,
                        abi_dynamic(data, 32) AS c
                    from logs
                    where chain = 1
                    and topics [1] = '\xc64a40e125a06afb756e3721cfa09bbcbccf1703151b93b4b303bb1a4198b2ea'
                )
                select
                    abi_uint_array(b) AS b,
                    abi_int_array(c) AS c
                from foo
                where a = '\x00000000000000000000000000000000000000000000000000000000deadbeef'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_erc20_sql() {
        check_sql(
            vec!["\r\nTransfer(address indexed from, address indexed to, uint tokens)\r\n"],
            r#"select "from", "to", tokens from transfer"#,
            r#"
                with transfer as not materialized (
                    select
                        topics[2] as "from",
                        topics[3] as "to",
                        abi_fixed_bytes(data, 0, 32) AS tokens
                    from logs
                    where chain = 1
                    and topics [1] = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                )
                select
                    abi_address("from") as "from",
                    abi_address("to") as "to",
                    abi_uint(tokens) as tokens
                from transfer
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_case() {
        check_sql(
                vec!["Foo(uint bar, uint baz)"],
                r#"
                    select
                        sum(case when bar = 0 then baz * -1 else 0 end) a,
                        sum(case when bar = 1 then baz else 0 end) b
                    from foo
                "#,
                r#"
                    with foo as not materialized (
                        select
                            abi_fixed_bytes(data, 0, 32) as bar,
                            abi_fixed_bytes(data, 32, 32) as baz
                        from logs
                        where chain = 1
                        and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                    )
                    select
                        sum(
                            case
                            when bar = '\x0000000000000000000000000000000000000000000000000000000000000000'
                            then abi_uint(baz) * -1
                            else 0
                            end
                        ) as a,
                        sum(
                            case
                            when bar = '\x0000000000000000000000000000000000000000000000000000000000000001'
                            then abi_uint(baz)
                            else 0
                            end
                        ) as b
                from foo
                "#,
            ).await;
    }

    #[tokio::test]
    async fn test_joins() {
        check_sql(
            vec!["Foo(uint a, uint b)", "Bar(uint a, uint b)"],
            r#"select t1.b, t2.b from foo t1 left outer join bar t2 on t1.a = t2.a"#,
            r#"
                with
                bar as not materialized (
                    select
                        abi_fixed_bytes(data, 0, 32) as a,
                        abi_fixed_bytes(data, 32, 32) as b
                    from logs
                    where chain = 1
                    and topics [1] = '\xde24c8e88b6d926d4bd258eddfb15ef86337654619dec5f604bbdd9d9bc188ca'
                ),
                foo as not materialized (
                    select
                        abi_fixed_bytes(data, 0, 32) as a,
                        abi_fixed_bytes(data, 32, 32) as b
                    from logs
                    where chain = 1
                    and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select abi_uint(t1.b) as b, abi_uint(t2.b) as b
                from foo as t1
                left join bar as t2
                on t1.a = t2.a
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_joins_on_single_table() {
        check_sql(
            vec!["Foo(uint indexed a, uint indexed b)"],
            r#"
                select t1.b, t1.block_num, t2.b
                from foo t1
                left outer join foo t2
                on t1.a = t2.a
                and t1.block_num < t2.block_num
            "#,
            r#"
                with
                foo as not materialized (
                    select
                        block_num,
                        topics [2] AS a,
                        topics [3] AS b
                    from logs
                    where chain = 1
                    and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select
                    abi_uint(t1.b) AS b,
                    t1.block_num,
                    abi_uint(t2.b) AS b
                from foo as t1
                left join foo as t2
                on t1.a = t2.a
                and t1.block_num < t2.block_num
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_joins_with_unselected() {
        check_sql(
            vec!["Foo(uint a, uint b)", "Bar(uint a, uint b)"],
            r#"select foo.b from foo"#,
            r#"
                with foo as not materialized (
                    select abi_fixed_bytes(data, 32, 32) as b
                    from logs
                    where chain = 1
                    and topics [1] = '\x36af629ed92d12da174153c36f0e542f186a921bae171e0318253e5a717234ea'
                )
                select abi_uint(foo.b) as b from foo
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_tmr_news() {
        check_sql(vec!["PredictionAdded(uint256 indexed marketId, uint256 indexed predictionId, address indexed predictor, uint256 value, string text, int256[] embedding)"],
            r#"
                select
                    address,
                    block_num,
                    "marketId",
                    "predictionId",
                    "predictor",
                    "value",
                    "text",
                    "embedding"
                FROM predictionadded
                WHERE address = '\x6e5310adD12a6043FeE1FbdC82366dcaB7f5Ad15'
            "#,
            r#"
                with predictionadded as not materialized (
                    select
                        address,
                        block_num,
                        topics[2] as "marketId",
                        topics[3] as "predictionId",
                        topics[4] as "predictor",
                        abi_fixed_bytes(data, 0, 32) as "value",
                        abi_bytes(abi_dynamic(data, 32)) as "text",
                        abi_dynamic(data, 64) as "embedding"
                    from logs
                    where chain = 1
                    and topics[1] = '\xce9c0df4181cf7f57cf163a3bc9d3102b1af09f4dcfed92644a72f5ca70fdfdf'
                )
                SELECT
                    address,
                    block_num,
                    abi_uint("marketId") AS "marketId",
                    abi_uint("predictionId") AS "predictionId",
                    abi_address("predictor") AS "predictor",
                    abi_uint("value") as "value",
                    abi_string("text") as "text",
                    abi_int_array("embedding") as "embedding"
                FROM predictionadded
                WHERE address = '\x6e5310add12a6043fee1fbdc82366dcab7f5ad15'
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_mud_query() {
        check_sql(
            vec!["Store_SetRecord(bytes32 indexed tableId, bytes32[] keyTuple, bytes staticData, bytes32 encodedLengths, bytes dynamicData)"],
            r#"select tableId, keyTuple, staticData, encodedLengths, dynamicData from store_setrecord"#,
            r#"
                with store_setrecord as not materialized (
                    select
                        topics [2] as tableid,
                        abi_dynamic(data, 0) as keytuple,
                        abi_bytes(abi_dynamic(data, 32)) as staticdata,
                        abi_fixed_bytes(data, 64, 32) as encodedlengths,
                        abi_bytes(abi_dynamic(data, 96)) as dynamicdata
                    from logs
                    where chain = 1
                    and topics [1] = '\x8dbb3a9672eebfd3773e72dd9c102393436816d832c7ba9e1e1ac8fcadcac7a9'
                )
                select
                    tableid,
                    abi_fixed_bytes_array(keytuple, 32) as keytuple,
                    staticdata,
                    encodedlengths,
                    dynamicdata
                from store_setrecord
            "#,
        ).await;
    }

    #[tokio::test]
    async fn test_seaport_query() {
        check_sql(
            vec!["OrderFulfilled(bytes32 orderHash, address indexed offerer, address indexed zone, address recipient, (uint8, address, uint256, uint256)[] offer, (uint8, address, uint256, uint256, address)[] consideration)"],
            r#"select orderHash, offerer, zone, recipient, offer, consideration from orderfulfilled"#,
            r#"
                with orderfulfilled as not materialized (
                select
                    topics [2] as offerer,
                    topics [3] as zone,
                    abi_fixed_bytes(data, 0, 32) as orderhash,
                    abi_fixed_bytes(data, 32, 32) as recipient,
                    (
                    select
                        json_agg(
                        json_build_object(
                            '0',
                            abi_fixed_bytes(data, 0, 32),
                            '1',
                            abi_fixed_bytes(data, 32, 32),
                            '2',
                            abi_fixed_bytes(data, 64, 32),
                            '3',
                            abi_fixed_bytes(data, 96, 32)
                        )
                        )
                    from
                        unnest(
                            abi_fixed_bytes_array(abi_dynamic(data, 64), 128)
                        ) as data
                    ) as offer,
                    (
                    select
                        json_agg(
                        json_build_object(
                            '0',
                            abi_fixed_bytes(data, 0, 32),
                            '1',
                            abi_fixed_bytes(data, 32, 32),
                            '2',
                            abi_fixed_bytes(data, 64, 32),
                            '3',
                            abi_fixed_bytes(data, 96, 32),
                            '4',
                            abi_fixed_bytes(data, 128, 32)
                        )
                        )
                    from
                        unnest(
                        abi_fixed_bytes_array(abi_dynamic(data, 96), 160)
                        ) as data
                    ) as consideration
                from logs
                where chain = 1
                and topics [1] = '\x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
                )
                select
                orderhash,
                abi_address(offerer) as offerer,
                abi_address(zone) as zone,
                abi_address(recipient) as recipient,
                offer,
                consideration
                from orderfulfilled
            "#
        ).await;
    }
}
