use std::collections::VecDeque;

use alloy::{
    hex::ToHexExt,
    primitives::{keccak256, FixedBytes, U256, U64},
};
use eyre::{eyre, OptionExt, Result};
use itertools::Itertools;
use sqlparser::ast::{self, Ident};

use crate::s256;

pub fn parse(input: &str) -> Result<Param> {
    let input = input.trim();
    let input = input.strip_prefix("event").unwrap_or(input);
    let rewritten = input
        .split_once('(')
        .map(|(name, tuple)| format!("({} {}", tuple, name))
        .ok_or_else(|| eyre!("missing tuple for event signature"))?;
    Param::parse(&mut Token::lex(&rewritten)?)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    OpenParen,
    CloseParen,
    Word(String),
    Array(Option<usize>),
    Comma,
}

impl Token {
    fn lex(input: &str) -> Result<VecDeque<Token>> {
        fn valid_char(c: char) -> bool {
            c.is_ascii_digit() || c.is_ascii_lowercase() || c.is_ascii_uppercase() || c == '_'
        }

        let mut tokens = Vec::new();
        let mut chars = input.chars().peekable();
        while let Some(&c) = chars.peek() {
            tokens.push(match c {
                c if c.is_whitespace() => {
                    chars.next();
                    continue;
                }
                '(' => {
                    chars.next();
                    Token::OpenParen
                }
                ')' => {
                    chars.next();
                    Token::CloseParen
                }
                '[' => {
                    chars.next();
                    let num: String = chars
                        .by_ref()
                        .peeking_take_while(|&c| c.is_ascii_digit())
                        .collect();
                    chars.next();
                    if num.is_empty() {
                        Token::Array(None)
                    } else {
                        Token::Array(Some(num.parse()?))
                    }
                }
                ',' => {
                    chars.next();
                    Token::Comma
                }
                c if valid_char(c) => {
                    let word: String = chars
                        .by_ref()
                        .peeking_take_while(|&c| valid_char(c))
                        .collect();
                    Token::Word(word)
                }
                c => return Err(eyre!("Unexpected character: {}", c)),
            });
        }
        Ok(VecDeque::from(tokens))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Kind {
    Tuple(Vec<Kind>),
    Array(Option<usize>, Box<Kind>),

    Address,
    Bool,
    Bytes(Option<usize>),
    Int(u16),
    Uint(u16),
    String,
}

/// Instructs to_sql on if it should decode to the final type
/// You may not want to do this for performancen reasons. Since
/// there are BTREE indexes on columns like topics[ i ] and address
/// you do not want to convert that data to the final type,
/// instead you want to leave it in raw bytes form, rewrite the
/// predicates to be in raw bytes form as well, and then once
/// the data set has been properly filtered, we can do the final
/// decoding as the last step.
pub enum Decode {
    Yes,
    No,
}

impl Kind {
    fn is_static(&self) -> bool {
        match &self {
            Kind::Tuple(fields) => fields.iter().all(Self::is_static),
            Kind::Array(Some(_), kind) => kind.is_static(),
            Kind::Array(None, _) => false,
            Kind::Address => true,
            Kind::Bool => true,
            Kind::Bytes(Some(_)) => true,
            Kind::Bytes(None) => false,
            Kind::Int(_) => true,
            Kind::Uint(_) => true,
            Kind::String => false,
        }
    }

    /// number of evm words occupied by the kind
    /// will always be a multiple of 32
    /// most of the time it _is_ 32 unless there
    /// is a static array or static tuple
    fn size(&self) -> usize {
        match &self {
            Kind::Tuple(fields) if self.is_static() => fields.iter().map(Self::size).sum(),
            Kind::Array(Some(size), kind) if kind.is_static() => 32 + size * kind.size(),
            Kind::Tuple(_)
            | Kind::Array(_, _)
            | Kind::Address
            | Kind::Bool
            | Kind::Bytes(_)
            | Kind::Int(_)
            | Kind::Uint(_)
            | Kind::String => 32,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Param {
    pub name: ast::Ident,
    pub kind: Kind,
    indexed: bool,
    components: Option<Vec<Param>>,
    element: Option<Box<Param>>,
    selected: Option<bool>,
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Kind::Tuple(kinds) => {
                write!(f, "(")?;
                for (i, k) in kinds.iter().enumerate() {
                    k.fmt(f)?;
                    if i != kinds.len() - 1 {
                        write!(f, ",")?;
                    }
                }
                write!(f, ")")
            }
            Kind::Array(None, kind) => {
                kind.fmt(f)?;
                write!(f, "[]")
            }
            Kind::Array(Some(size), kind) => {
                kind.fmt(f)?;
                write!(f, "[{}]", size)
            }
            Kind::Address => write!(f, "address"),
            Kind::Bool => write!(f, "bool"),
            Kind::Bytes(Some(size)) => write!(f, "bytes{}", size),
            Kind::Bytes(None) => write!(f, "bytes"),
            Kind::Int(bits) => write!(f, "int{}", bits),
            Kind::Uint(bits) => write!(f, "uint{}", bits),
            Kind::String => write!(f, "string"),
        }
    }
}

impl Param {
    fn new(name: &str, kind: Kind) -> Param {
        Param {
            kind,
            name: Ident::new(name),
            indexed: false,
            components: None,
            element: None,
            selected: None,
        }
    }

    fn from_components(name: &str, components: Vec<Param>) -> Param {
        Param {
            name: Ident::new(name),
            kind: Kind::Tuple(components.iter().map(|c| c.kind.clone()).collect()),
            indexed: false,
            components: Some(components),
            element: None,
            selected: None,
        }
    }

    fn parse(input: &mut VecDeque<Token>) -> Result<Param> {
        let mut param = match input.pop_front() {
            Some(Token::OpenParen) => {
                let mut components = Vec::new();
                while let Some(token) = input.front() {
                    match token {
                        Token::OpenParen | Token::Word(_) => {
                            components.push(Param::parse(input)?);
                        }
                        Token::Comma => {
                            input.pop_front();
                        }
                        Token::CloseParen => {
                            input.pop_front();
                            break;
                        }
                        _ => {
                            return Err(eyre!("expected '(', word, ',', or ')'. got: {:?}", token))
                        }
                    }
                }
                Param::from_components("", components)
            }
            Some(Token::Word(type_desc)) => {
                if let Some(bits) = type_desc.strip_prefix("int") {
                    Param::new("", Kind::Int(bits.parse().unwrap_or(256)))
                } else if let Some(bits) = type_desc.strip_prefix("uint") {
                    Param::new("", Kind::Uint(bits.parse().unwrap_or(256)))
                } else if let Some(bytes) = type_desc.strip_prefix("bytes") {
                    if bytes.is_empty() {
                        Param::new("", Kind::Bytes(None))
                    } else {
                        Param::new("", Kind::Bytes(Some(bytes.parse()?)))
                    }
                } else if type_desc == "address" {
                    Param::new("", Kind::Address)
                } else if type_desc == "bool" {
                    Param::new("", Kind::Bool)
                } else if type_desc == "string" {
                    Param::new("", Kind::String)
                } else {
                    return Err(eyre!("{} not yet implemented", type_desc));
                }
            }
            None => return Err(eyre!("eof")),
            _ => return Err(eyre!("expected '(' or word")),
        };
        while let Some(Token::Array(size)) = input.front() {
            param.element.get_or_insert(Box::new(param.clone()));
            param.kind = Kind::Array(*size, Box::new(param.kind.clone()));
            param.components = None;
            input.pop_front();
        }
        match input.front() {
            Some(Token::Word(word)) if word == "indexed" => {
                input.pop_front();
                param.indexed = true;
                match input.pop_front() {
                    Some(Token::Word(name)) => {
                        param.name = Ident::new(name);
                        Ok(param)
                    }
                    Some(_) | None => Err(eyre!("missing name for {:?}", param.kind)),
                }
            }
            Some(Token::Word(word)) => {
                param.name = Ident::new(word);
                input.pop_front();
                Ok(param)
            }
            Some(_) | None => Err(eyre!("missing name for {:?}", param.kind)),
        }
    }

    pub fn sighash(&self) -> FixedBytes<32> {
        keccak256(format!("{}{}", self.name, self.kind))
    }

    /// Query must start with outermost param's name.
    /// If found returns the param that was selected
    /// and may not be the outermost param.
    pub fn find(&mut self, query: Vec<Ident>) -> Option<&mut Self> {
        if query.is_empty() {
            return None;
        }
        if self.name.value != query[0].value {
            return None;
        }
        if query.len() == 1 {
            self.name.quote_style = query[0].quote_style;
            return Some(self);
        }
        self.components
            .iter_mut()
            .flatten()
            .find_map(|c| c.find(query.iter().skip(1).cloned().collect()))
    }

    pub fn select(&mut self) {
        self.selected = Some(true);
        self.components
            .iter_mut()
            .flatten()
            .for_each(|c| c.select());
    }

    /// true when this param, or any of its components, are selected = Some(True)
    pub fn selected(&self) -> bool {
        self.selected.unwrap_or(
            self.components
                .as_ref()
                .map(|components| components.iter().any(Param::selected))
                .unwrap_or(false),
        )
    }

    pub fn topics_to_sql(&self) -> Vec<(Ident, String)> {
        self.components
            .iter()
            .flat_map(|v| v.iter())
            .enumerate()
            .filter(|(_, param)| param.indexed && param.selected())
            .map(|(pos, param)| (param.name.clone(), format!("topics[{}]", pos + 2)))
            .collect()
    }

    pub fn to_sql(&self, inner: &str) -> Vec<(Ident, String)> {
        self.components
            .iter()
            .flatten()
            .filter(|p| !p.indexed)
            .scan(0, |size_counter, param| {
                let size = *size_counter;
                *size_counter += param.kind.size();
                Some((size, param))
            })
            .filter(|(_, param)| param.selected())
            .map(|(pos, param)| match &param.kind {
                Kind::Tuple(_) => todo!(),
                Kind::Array(_, _) => todo!(),
                Kind::Bytes(None) | Kind::String => (
                    param.name.clone(),
                    format!("abi_bytes(abi_dynamic({}, {}))", inner, pos),
                ),
                Kind::Address
                | Kind::Bool
                | Kind::Bytes(Some(_))
                | Kind::Int(_)
                | Kind::Uint(_) => (
                    param.name.clone(),
                    format!("abi_fixed_bytes({}, {}, {})", inner, pos, param.kind.size()),
                ),
            })
            .collect()
    }
}

trait AbiBytes {
    fn next_usize(&self, offset: usize) -> Result<usize>;
    fn skip_to(&self, offset: usize) -> Result<&[u8]>;
    fn get_static(&self, offset: usize, length: usize) -> Result<&[u8]>;
    fn get_dynamic(&self, offset: usize) -> Result<&[u8]>;
}

impl AbiBytes for &[u8] {
    fn next_usize(&self, i: usize) -> Result<usize> {
        Ok(U64::from_be_slice(&self.get_static(i, 32)?[24..32]).to())
    }

    fn get_static(&self, i: usize, length: usize) -> Result<&[u8]> {
        self.get(i..i + length).ok_or_eyre("eof")
    }

    fn get_dynamic(&self, i: usize) -> Result<&[u8]> {
        self.get(32..32 + self.next_usize(i)?).ok_or_eyre("eof")
    }

    fn skip_to(&self, i: usize) -> Result<&[u8]> {
        self.get(self.next_usize(i)?..).ok_or_eyre("eof")
    }
}

pub fn to_json(input: &[u8], param: &Param) -> Result<serde_json::Value> {
    match &param.kind {
        Kind::Array(length, kind) => Ok(serde_json::Value::Array(
            (0..length.unwrap_or(input.next_usize(0)?))
                .map(|i| {
                    if kind.is_static() {
                        Ok(to_json(
                            input.get_static(i * kind.size(), kind.size())?,
                            param.element.as_ref().unwrap(),
                        )?)
                    } else {
                        Ok(to_json(
                            (&input[32..]).skip_to(32 * i)?,
                            param.element.as_ref().unwrap(),
                        )?)
                    }
                })
                .collect::<Result<_>>()?,
        )),
        Kind::Tuple(_) => Ok(serde_json::Value::Object(
            param
                .components
                .iter()
                .flatten()
                .filter(|p| !p.indexed)
                .scan(0, |offset, c| {
                    let i = *offset;
                    *offset += param.kind.size();
                    Some((i, c))
                })
                .map(|(i, c)| {
                    let field = if c.kind.is_static() {
                        to_json(input.get_static(i, c.kind.size())?, c)?
                    } else {
                        to_json(input.skip_to(i)?, c)?
                    };
                    Ok((c.name.to_string(), field))
                })
                .collect::<Result<_>>()?,
        )),
        Kind::Address => Ok(input.get_static(12, 20)?.encode_hex().into()),
        Kind::Bool => Ok((input.get_static(31, 1)?[0] == 1).into()),
        Kind::Bytes(None) => Ok(input.get_dynamic(0)?.encode_hex().into()),
        Kind::Bytes(Some(size)) => Ok(input.get_static(0, *size)?.encode_hex().into()),
        Kind::String => Ok(String::from_utf8(input.get_dynamic(0)?.to_vec())?.into()),
        Kind::Int(_) => Ok(s256::Int::try_from_be_slice(input.get_static(0, 32)?)
            .ok_or_eyre("decoding i256")?
            .to_string()
            .into()),
        Kind::Uint(_) => Ok(U256::try_from_be_slice(input.get_static(0, 32)?)
            .ok_or_eyre("decoding u256")?
            .to_string()
            .into()),
    }
}

#[cfg(test)]
mod json_tests {
    use super::{parse, to_json};
    use alloy::{hex, primitives::U256, sol, sol_types::SolEvent};
    use assert_json_diff::assert_json_eq;

    #[test]
    fn test_basic() {
        sol! {
            #[sol(abi)]
            event Foo(uint a, string[] b);
        };
        let data = Foo {
            a: U256::from(42),
            b: vec![String::from("hello"), String::from("world")],
        };
        let param = parse("Foo(uint256 a, string[] b)").unwrap();
        assert_json_eq!(
            to_json(&data.encode_log_data().data, &param).unwrap(),
            serde_json::json!({"a": "42", "b": ["hello", "world"]})
        )
    }

    #[test]
    fn test_advanced() {
        let data = hex!(
            r#"
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000060
            00000000000000000000000000000000000000000000000000000000000000a0
            0000000000000000000000000000000000000000000000000000000000000120
            0000000000000000000000000000000000000000000000000000000000000002
            4242000000000000000000000000000000000000000000000000000000000000
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000002
            4242000000000000000000000000000000000000000000000000000000000000
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000002
            4242000000000000000000000000000000000000000000000000000000000000
            "#
        );
        let param = parse("Foo((string b, string[] c, string[] d)[] a)").unwrap();
        assert_json_eq!(
            to_json(&data, &param).unwrap(),
            serde_json::json!({"a": [{"b": "BB", "c": ["BB"], "d": ["BB"]}]})
        )
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        hex::ToHexExt,
        primitives::{hex, U256},
        sol_types::SolEvent,
    };
    use assert_json_diff::assert_json_eq;

    use super::{parse, Kind, Token};
    use sqlparser::ast::Ident;

    macro_rules! ident {
        ($id:expr) => {{
            Ident::new($id)
        }};
        ($id:expr, $($rest:expr),+) => {{
            let mut v = vec![Ident::new($id)];
            $(v.push(Ident::new($rest));)*
            v
        }};
    }

    #[test]
    fn test_find() {
        assert!(parse("Foo(uint a, uint b)")
            .unwrap()
            .find(ident!("Foo", "a"))
            .is_some());
        assert!(parse("Foo(uint a, uint b, (uint c) d)")
            .unwrap()
            .find(ident!("Foo", "d", "c"))
            .is_some());
        assert!(parse("Foo(uint a, uint b)")
            .unwrap()
            .find(ident!("Foo", "c"))
            .is_none());
    }

    #[test]
    fn test_select_has_select() {
        assert!({
            let mut param = parse("Foo(uint a, uint b)").unwrap();
            param.find(ident!("Foo", "a")).unwrap().select();
            param.selected()
        });
        assert!({
            let mut param = parse("Foo(uint a, uint b, (uint c) d)").unwrap();
            param.find(ident!("Foo", "d", "c")).unwrap().select();
            param.selected()
        });
        assert!({
            let mut param = parse("Foo(uint a, uint b)").unwrap();
            param.find(ident!("Foo", "c")).is_none() && !param.selected()
        });
    }

    #[test]
    fn test_sighash() {
        assert_eq!(
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            parse("Transfer(address from, address to, uint256 value)")
                .unwrap()
                .sighash()
                .encode_hex()
        );
        assert_eq!(
            "8dbb3a9672eebfd3773e72dd9c102393436816d832c7ba9e1e1ac8fcadcac7a9",
            parse(
                //underscores
                r#"
                Store_SetRecord(
                    bytes32 indexed tableId,
                    bytes32[] keyTuple,
                    bytes staticData,
                    bytes32 encodedLengths,
                    bytes dynamicData
                )
            "#
            )
            .unwrap()
            .sighash()
            .encode_hex()
        );
        assert_eq!(
            "30ebccc1ba352c4539c811df296809a7ae8446c4965445b6ee359b7a47f1bc8f",
            parse(
                r#"
                    IntentFinished(
                        address indexed intentAddr,
                        address indexed destinationAddr,
                        bool indexed success,
                        (
                            uint256 toChainId,
                            (address token, uint256 amount)[] bridgeTokenOutOptions,
                            (address token, uint256 amount) finalCallToken,
                            (address to, uint256 value, bytes data) finalCall,
                            address escrow,
                            address refundAddress,
                            uint256 nonce
                        ) intent
                    )
                "#
            )
            .unwrap()
            .sighash()
            .encode_hex()
        )
    }

    #[test]
    fn test_kind_display() {
        assert_eq!("int256", Kind::Int(256).to_string());
        assert_eq!(
            "int256[1]",
            Kind::Array(Some(1), Box::new(Kind::Int(256))).to_string()
        );
        assert_eq!(
            "int256[]",
            Kind::Array(None, Box::new(Kind::Int(256))).to_string()
        );
        assert_eq!(
            "(int256[],bytes)",
            Kind::Tuple(vec![
                Kind::Array(None, Box::new(Kind::Int(256))),
                Kind::Bytes(None)
            ])
            .to_string()
        );
    }

    #[test]
    fn test_static() {
        assert!(Kind::Int(256).is_static());
        assert!(Kind::Array(Some(1), Box::new(Kind::Int(256))).is_static());
        assert!(!Kind::Array(None, Box::new(Kind::Int(256))).is_static());
    }

    #[test]
    fn test_lex() {
        assert_eq!(
            Token::lex("(foo bar, baz qux)").unwrap(),
            vec![
                Token::OpenParen,
                Token::Word(String::from("foo")),
                Token::Word(String::from("bar")),
                Token::Comma,
                Token::Word(String::from("baz")),
                Token::Word(String::from("qux")),
                Token::CloseParen,
            ]
        );
        assert_eq!(
            Token::lex("(hello[][] world)[42]").unwrap(),
            vec![
                Token::OpenParen,
                Token::Word(String::from("hello")),
                Token::Array(None),
                Token::Array(None),
                Token::Word(String::from("world")),
                Token::CloseParen,
                Token::Array(Some(42))
            ]
        );
        assert_eq!(
            Token::lex("(hello world)[42]").unwrap(),
            vec![
                Token::OpenParen,
                Token::Word(String::from("hello")),
                Token::Word(String::from("world")),
                Token::CloseParen,
                Token::Array(Some(42))
            ]
        );
    }

    #[test]
    fn test_param_parse() {
        assert_eq!(&parse("foo(int bar)").unwrap().kind.to_string(), "(int256)");
        assert_eq!(
            parse("Foo(string a, bytes16 b, bytes c, int256 d, int256[] e, string[] f, bool g)")
                .unwrap()
                .kind
                .to_string(),
            "(string,bytes16,bytes,int256,int256[],string[],bool)"
        );
        assert_eq!(
            parse("(int[][] bar)[] foo").unwrap().kind.to_string(),
            "(int256[][])[]"
        );
    }

    #[test]
    fn test_to_sql() {
        assert_eq!(
            vec![(ident!("b"), String::from("abi_bytes(abi_dynamic(data, 0))"))],
            {
                let mut param = parse("foo(int indexed a, bytes b)").unwrap();
                param.find(ident!("foo", "b")).unwrap().select();
                param.to_sql("data")
            }
        );
        assert_eq!(
            vec![(ident!("a"), String::from("abi_fixed_bytes(data, 0, 32)"))],
            {
                let mut param = parse("(int a ) foo").unwrap();
                param.find(ident!("foo", "a")).unwrap().select();
                param.to_sql("data")
            }
        );
        assert_eq!(
            vec![(
                ident!("a"),
                String::from(
                    "json_build_object('b',encode(abi_bytes(abi_dynamic(abi_dynamic(data, 0), 0)), 'hex'),'c',abi_int(abi_fixed_bytes(abi_dynamic(data, 0), 32, 32))::text)"
                )
            )],
            {
                let mut param = parse("((bytes b, int c) a) foo").unwrap();
                param.find(ident!("foo", "a")).unwrap().select();
                param.to_sql("data")
            }
        );
        assert_eq!(
            vec![(
                ident!("c"),
                String::from("json_build_object('d',abi_uint(abi_fixed_bytes(abi_fixed_bytes(data, 32, 32), 0, 32))::text)")
            )],
            {
                let mut param = parse("((bytes b) a, (uint d) c) foo").unwrap();
                param.find(ident!("foo", "c")).unwrap().select();
                    param.to_sql("data")
            }
        );
    }

    static SCHEMA: &str = include_str!("./sql/schema.sql");

    #[tokio::test]
    async fn test_static_array() {
        let (_pg_server, pool) = shared::pg::test::new(SCHEMA).await;
        let pg = pool.get().await.expect("getting pg from test pool");
        let data = hex!(
            r#"
            0000000000000000000000000000000000000000000000000000000000000005
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000002
            0000000000000000000000000000000000000000000000000000000000000003
            0000000000000000000000000000000000000000000000000000000000000004
            0000000000000000000000000000000000000000000000000000000000000005
            "#
        );
        let mut param = parse("(uint[5] b) a").unwrap();
        param.find(ident!("a", "b")).unwrap().select();
        let row = pg
            .query_one(
                &format!(
                    "with data as (select {} as b) select abi_uint_array(b) from data",
                    param.to_sql("$1")[0].1
                ),
                &[&data],
            )
            .await
            .expect("issue with query");
        let res: Vec<U256> = row.get(0);
        assert_eq!(
            vec![
                U256::from(1),
                U256::from(2),
                U256::from(3),
                U256::from(4),
                U256::from(5)
            ],
            res
        )
    }

    #[tokio::test]
    async fn test_abi_uint_array() {
        let (_pg_server, pool) = shared::pg::test::new(SCHEMA).await;
        let pg = pool.get().await.expect("getting pg from test pool");
        let data = hex!(
            r#"
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000005
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000002
            0000000000000000000000000000000000000000000000000000000000000003
            0000000000000000000000000000000000000000000000000000000000000004
            0000000000000000000000000000000000000000000000000000000000000005
            "#
        );
        let mut param = parse("(uint[] b) a").unwrap();
        param.find(ident!("a", "b")).unwrap().select();
        let row = pg
            .query_one(
                &format!(
                    "with data as (select {} as b) select abi_uint_array(b) from data",
                    param.to_sql("$1")[0].1
                ),
                &[&data],
            )
            .await
            .expect("issue with query");
        let res: Vec<U256> = row.get(0);
        assert_eq!(
            vec![
                U256::from(1),
                U256::from(2),
                U256::from(3),
                U256::from(4),
                U256::from(5)
            ],
            res
        )
    }

    #[tokio::test]
    async fn test_complex_event() {
        let (_pg_server, pool) = shared::pg::test::new(SCHEMA).await;
        let pg = pool.get().await.expect("getting pg from test pool");
        let data = hex!(
            r#"
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000002105
            0000000000000000000000000000000000000000000000000000000000000100
            000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913
            00000000000000000000000000000000000000000000000000000000000f4240
            00000000000000000000000000000000000000000000000000000000000001e0
            0000000000000000000000009bd9caf29b76e98d57fc3a228a39c7efe8ca0eaf
            0000000000000000000000007531f00dbc616b3466990e615bf01eff507c88d4
            4f24c5540ed51ae10044296e2974edba583788db5bb132ff2e0339770ca018b8
            0000000000000000000000000000000000000000000000000000000000000003
            000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913
            00000000000000000000000000000000000000000000000000000000000f4240
            000000000000000000000000eb466342c4d449bc9f53a865d5cb90586f405215
            00000000000000000000000000000000000000000000000000000000000f4240
            00000000000000000000000050c5725949a6f0c72e6c4a641f24049a917db0cb
            0000000000000000000000000000000000000000000000000de0b6b3a7640000
            0000000000000000000000007531f00dbc616b3466990e615bf01eff507c88d4
            0000000000000000000000000000000000000000000000000000000000000000
            0000000000000000000000000000000000000000000000000000000000000060
            0000000000000000000000000000000000000000000000000000000000000005
            68656C6C6F000000000000000000000000000000000000000000000000000000
            "#
        );
        let mut param = parse("IntentFinished(address indexed intentAddr, address indexed destinationAddr, bool indexed success,(uint256 toChainId, (address token, uint256 amount)[] bridgeTokenOutOptions, (address token, uint256 amount) finalCallToken, (address to, uint256 value, bytes data) finalCall, address escrow, address refundAddress, uint256 nonce) intent)").unwrap();
        param
            .find(ident!("IntentFinished", "intent"))
            .unwrap()
            .select();
        let query = param.to_sql("$1");
        let row = pg
            .query_one(&format!("select {}", &query[0].1), &[&data])
            .await
            .expect("issue with query");
        let res: serde_json::Value = row.get(0);
        assert_json_eq!(
            res,
            serde_json::json!({
                "toChainId": "8453",
                "bridgeTokenOutOptions": [
                    {
                        "token": "833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                        "amount": "1000000"
                    },
                    {
                        "token": "eb466342c4d449bc9f53a865d5cb90586f405215",
                        "amount": "1000000"
                    },
                    {
                        "token": "50c5725949a6f0c72e6c4a641f24049a917db0cb",
                        "amount": "1000000000000000000"
                    }
                ],
                "finalCallToken": {
                    "token": "833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                    "amount": "1000000"
                },
                "finalCall": {
                    "to": "7531f00dbc616b3466990e615bf01eff507c88d4",
                    "value": "0",
                    "data": "68656c6c6f",
                },
                "escrow": "9bd9caf29b76e98d57fc3a228a39c7efe8ca0eaf",
                "refundAddress": "7531f00dbc616b3466990e615bf01eff507c88d4",
                "nonce": "35797683442637942692858199402223327241210246169636214527328521135655386880184"
            })
        )
    }

    fn print_hex(s: &str) {
        let out = s
            .chars()
            .collect::<Vec<_>>()
            .chunks(64)
            .map(|c| c.iter().collect::<String>())
            .collect::<Vec<_>>()
            .join("\n");
        println!("{}", out);
    }

    #[test]
    fn test_gen_data() {
        alloy::sol! {
            #[sol(abi)]
            event Foo((string, string[], string[])[] a);
        };
        let foo = Foo {
            a: vec![(
                String::from("BB"),
                vec![String::from("BB")],
                vec![String::from("BB")],
            )],
        };
        print_hex(&foo.encode_data().encode_hex());
    }

    #[tokio::test]
    async fn test_complex_event_dynamic_array() {
        let (_pg_server, pool) = shared::pg::test::new(SCHEMA).await;
        let pg = pool.get().await.expect("getting pg from test pool");
        let data = hex!(
            r#"
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000060
            00000000000000000000000000000000000000000000000000000000000000a0
            0000000000000000000000000000000000000000000000000000000000000120
            0000000000000000000000000000000000000000000000000000000000000002
            4242000000000000000000000000000000000000000000000000000000000000
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000002
            4242000000000000000000000000000000000000000000000000000000000000
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000002
            4242000000000000000000000000000000000000000000000000000000000000
            "#
        );
        let mut param = parse("Foo((string b, string[] c, string[] d)[] a)").unwrap();
        param.find(ident!("Foo", "a")).unwrap().select();
        println!("{:?}", param);
        let query = param.to_sql("$1");
        println!("query: {}", fmt_sql(&query[0].1));
        let row = pg
            .query_one(&format!("select {}", &query[0].1), &[&data])
            .await
            .expect("issue with query");
        let res: serde_json::Value = row.get(0);
        assert_json_eq!(
            res,
            serde_json::json!({
                "a": [
                    {
                        "b": "BB",
                        "c": ["BB"],
                        "d": ["BB"]
                    }
                ]
            })
        )
    }

    const PG: &sqlparser::dialect::PostgreSqlDialect = &sqlparser::dialect::PostgreSqlDialect {};
    fn fmt_sql(sql: &str) -> String {
        match sqlparser::parser::Parser::parse_sql(PG, sql) {
            Ok(ast) => sqlformat::format(
                &ast[0].to_string(),
                &sqlformat::QueryParams::None,
                sqlformat::FormatOptions::default(),
            ),
            Err(_) => sql.to_string(),
        }
    }
}
