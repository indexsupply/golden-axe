#![allow(dead_code)]
use std::collections::VecDeque;

use alloy::primitives::{keccak256, FixedBytes};
use eyre::{eyre, Result};
use itertools::Itertools;

fn parse(input: &str) -> Result<Param> {
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
    Array(Option<u16>),
    Comma,
}

impl Token {
    fn lex(input: &str) -> Result<VecDeque<Token>> {
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
                c if c.is_ascii_alphanumeric() => {
                    let word: String = chars
                        .by_ref()
                        .peeking_take_while(|&c| c.is_ascii_alphanumeric())
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
enum Kind {
    Tuple(Vec<Kind>),
    Array(Option<u16>, Box<Kind>),

    Address,
    Bool,
    Bytes(Option<u16>),
    Int(u16),
    Uint(u16),
    String,
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

    fn size(&self) -> u16 {
        match &self {
            Kind::Tuple(fields) if self.is_static() => fields.iter().map(Self::size).sum(),
            Kind::Tuple(_) => 32,
            Kind::Array(Some(size), kind) if kind.is_static() => 32 + size * kind.size(),
            Kind::Array(Some(_), _) => 32,
            Kind::Array(None, _) => 32,
            Kind::Address => 20,
            Kind::Bool => 32,
            Kind::Bytes(Some(size)) => *size,
            Kind::Bytes(None) => 32,
            Kind::Int(size) => size / 8,
            Kind::Uint(size) => size / 8,
            Kind::String => 32,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Param {
    name: String,
    kind: Kind,
    indexed: bool,
    components: Option<Vec<Param>>,
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
            name: name.to_owned(),
            indexed: false,
            components: None,
            selected: None,
        }
    }

    fn indexed(name: &str, kind: Kind) -> Param {
        let mut param = Param::new(name, kind);
        param.indexed = true;
        param
    }

    fn from_components(name: &str, components: Vec<Param>) -> Param {
        Param {
            name: name.to_owned(),
            kind: Kind::Tuple(components.iter().map(|c| c.kind.clone()).collect()),
            indexed: false,
            components: Some(components),
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
                } else {
                    return Err(eyre!("{} not yet implemented", type_desc));
                }
            }
            None => return Err(eyre!("eof")),
            _ => return Err(eyre!("expected '(' or word")),
        };
        while let Some(Token::Array(size)) = input.front() {
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
                        param.name = name.clone();
                        Ok(param)
                    }
                    Some(_) | None => Err(eyre!("missing name for {:?}", param.kind)),
                }
            }
            Some(Token::Word(word)) => {
                param.name = word.clone();
                input.pop_front();
                Ok(param)
            }
            Some(_) | None => Err(eyre!("missing name for {:?}", param.kind)),
        }
    }

    fn sighash(&self) -> FixedBytes<32> {
        keccak256(format!("{}{}", self.name, self.kind))
    }

    fn has_select(&self) -> bool {
        self.selected.unwrap_or(
            self.components
                .as_ref()
                .map(|components| components.iter().any(Param::has_select))
                .unwrap_or(false),
        )
    }

    fn to_sql(&self, inner: String) -> Result<Vec<String>> {
        if let Some(components) = &self.components {
            let inner = if self.kind.is_static() {
                format!("abi_fixed_bytes({}, 0, {})", inner, self.kind.size())
            } else {
                format!("abi_dynamic({}, 0)", inner)
            };
            let mut result = Vec::new();
            let mut size_counter = 0;
            for (pos, param) in components.iter().enumerate() {
                if param.has_select() {
                    match &param.kind {
                        Kind::Tuple(_) => result.extend(param.to_sql(inner.clone())?),
                        Kind::Array(Some(_), kind) if kind.is_static() => {
                            result.push(format!(
                                "abi_fixed_bytes({}, {}, {})",
                                inner,
                                size_counter,
                                param.kind.size()
                            ));
                        }
                        Kind::Array(_, _) => {
                            result.push(format!("abi_dynamic({}, {})", inner, pos));
                        }
                        Kind::Bytes(None) | Kind::String => {
                            result.push(format!("abi_bytes(abi_dynamic({}, {}))", inner, pos));
                        }
                        Kind::Address
                        | Kind::Bool
                        | Kind::Bytes(Some(_))
                        | Kind::Uint(_)
                        | Kind::Int(_) => {
                            result.push(format!(
                                "abi_fixed_bytes({}, {}, {})",
                                inner,
                                size_counter,
                                param.kind.size()
                            ));
                        }
                    };
                }
                size_counter += param.kind.size();
            }
            Ok(result)
        } else {
            Err(eyre!("must provide tuple"))
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        hex::ToHexExt,
        primitives::{hex, U256},
    };

    use super::{parse, Kind, Param, Token};

    // test helper that will simulate a user
    // selecting certain fields in their query
    fn select(mut param: Param, query: &[&str]) -> Param {
        if param.name == query[0] {
            if query[1..].is_empty() {
                param.selected = Some(true);
            }
            if let Some(components) = param.components.clone() {
                let mut new = Vec::new();
                for c in components {
                    new.push(select(c.clone(), &query[1..]));
                }
                param.components = Some(new);
            }
        }
        param
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
        assert_eq!(
            Param::parse(&mut Token::lex("int foo").unwrap()).unwrap(),
            Param::new("foo", Kind::Int(256)),
        );
        assert_eq!(
            Param::parse(&mut Token::lex("int indexed foo").unwrap()).unwrap(),
            Param::indexed("foo", Kind::Int(256)),
        );
        assert_eq!(
            Param::parse(&mut Token::lex("(int bar) foo").unwrap()).unwrap(),
            Param::from_components("foo", vec![Param::new("bar", Kind::Int(256))])
        );
        assert_eq!(
            Param::parse(&mut Token::lex("(int[][] bar)[] foo").unwrap()).unwrap(),
            Param {
                name: String::from("foo"),
                kind: Kind::Array(
                    None,
                    Box::new(Kind::Tuple(vec![Kind::Array(
                        None,
                        Box::new(Kind::Array(None, Box::new(Kind::Int(256))))
                    )]))
                ),
                indexed: false,
                components: None,
                selected: None,
            }
        );
    }

    #[test]
    fn test_sql() {
        assert_eq!(
            vec!["abi_fixed_bytes(abi_fixed_bytes(data, 0, 32), 0, 32)"],
            select(parse("(int a) foo").unwrap(), &["foo", "a"])
                .to_sql(String::from("data"))
                .unwrap(),
        );
        assert_eq!(
            vec!["abi_fixed_bytes(abi_dynamic(abi_dynamic(data, 0), 0), 32, 32)"],
            select(
                parse("((bytes b, int c) a) foo").unwrap(),
                &["foo", "a", "c"]
            )
            .to_sql(String::from("data"))
            .unwrap(),
        );
        assert_eq!(
            vec!["abi_fixed_bytes(abi_fixed_bytes(abi_dynamic(data, 0), 0, 32), 0, 32)"],
            select(
                parse("((bytes b) a, (uint d) c) foo").unwrap(),
                &["foo", "c", "d"]
            )
            .to_sql(String::from("data"))
            .unwrap(),
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
        let param = select(
            parse("(uint[5] b) a").expect("unable to parse abi sig"),
            &["a", "b"],
        );
        let row = pg
            .query_one(
                &format!(
                    "select abi_uint_array({})",
                    param.to_sql(String::from("$1")).unwrap().first().unwrap()
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
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000005
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000002
            0000000000000000000000000000000000000000000000000000000000000003
            0000000000000000000000000000000000000000000000000000000000000004
            0000000000000000000000000000000000000000000000000000000000000005
            "#
        );
        let param = select(
            parse("(uint[] b) a").expect("unable to parse abi sig"),
            &["a", "b"],
        );
        let row = pg
            .query_one(
                &format!(
                    "select abi_uint_array({})",
                    param.to_sql(String::from("$1")).unwrap().first().unwrap()
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
}
