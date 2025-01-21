use std::collections::VecDeque;

use alloy::primitives::{keccak256, FixedBytes};
use eyre::{eyre, Result};
use itertools::Itertools;
use sqlparser::ast::{self, Ident};

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
    Array(Option<u16>),
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
            Kind::Array(Some(size), kind) if kind.is_static() => 32 + size * kind.size(),
            _ => 32,
            // Kind::Tuple(_) => 32,
            // Kind::Array(Some(_), _) => 32,
            // Kind::Array(None, _) => 32,
            // Kind::Address => 20,
            // Kind::Bool => 32,
            // Kind::Bytes(Some(_)) => 32,
            // Kind::Bytes(None) => 32,
            // Kind::Int(size) => size / 8,
            // Kind::Uint(size) => size / 8,
            // Kind::String => 32,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Param {
    pub name: ast::Ident,
    pub kind: Kind,
    pub indexed: bool,
    pub components: Option<Vec<Param>>,
    pub selected: Option<bool>,
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
            name: Ident::new(name),
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

    pub fn to_sql(&self, inner: &str) -> Vec<String> {
        let mut result = Vec::new();
        let indexed = self
            .components
            .iter()
            .flat_map(|v| v.iter())
            .enumerate()
            .filter(|(_, param)| param.indexed && param.selected());
        for (pos, param) in indexed {
            // PG indexes by 1. First topic is sighash
            result.push(format!("topics[{}] as {}", pos + 2, param.name));
        }
        let abi_encoded = self
            .components
            .iter()
            .flat_map(|v| v.iter())
            .filter(|p| !p.indexed)
            .scan(0, |size_counter, param| {
                let size = *size_counter;
                *size_counter += param.kind.size();
                Some((size, param))
            })
            .filter(|(_, param)| param.selected());
        for (size_counter, param) in abi_encoded {
            match &param.kind {
                Kind::Tuple(_) if param.kind.is_static() => {
                    result.extend(param.to_sql(&format!(
                        "abi_fixed_bytes({}, 0, {})",
                        inner,
                        self.kind.size()
                    )));
                }
                Kind::Tuple(_) => {
                    result
                        .extend(param.to_sql(&format!("abi_dynamic({}, {})", inner, size_counter)));
                }
                Kind::Array(Some(_), kind) if kind.is_static() => {
                    result.push(format!(
                        "abi_fixed_bytes({}, {}, {}) as {}",
                        inner,
                        size_counter,
                        param.kind.size(),
                        param.name
                    ));
                }
                Kind::Array(_, _) => {
                    result.push(format!(
                        "abi_dynamic({}, {}) as {}",
                        inner, size_counter, param.name
                    ));
                }
                Kind::Bytes(None) | Kind::String => {
                    result.push(format!(
                        "abi_bytes(abi_dynamic({}, {})) as {}",
                        inner, size_counter, param.name
                    ));
                }
                Kind::Address
                | Kind::Bool
                | Kind::Bytes(Some(_))
                | Kind::Uint(_)
                | Kind::Int(_) => {
                    result.push(format!(
                        "abi_fixed_bytes({}, {}, {}) as {}",
                        inner,
                        size_counter,
                        param.kind.size(),
                        param.name
                    ));
                }
            };
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        hex::ToHexExt,
        primitives::{hex, U256},
    };

    use super::{parse, Kind, Param, Token};
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
            parse("Foo(string a, bytes16 b, bytes c, int256 d, int256[] e, string[] f, bool g)")
                .unwrap(),
            Param::from_components(
                "Foo",
                vec![
                    Param::new("a", Kind::String),
                    Param::new("b", Kind::Bytes(Some(16))),
                    Param::new("c", Kind::Bytes(None)),
                    Param::new("d", Kind::Int(256)),
                    Param::new("e", Kind::Array(None, Box::new(Kind::Int(256)))),
                    Param::new("f", Kind::Array(None, Box::new(Kind::String))),
                    Param::new("g", Kind::Bool),
                ]
            )
        );
        assert_eq!(
            Param::parse(&mut Token::lex("(int[][] bar)[] foo").unwrap()).unwrap(),
            Param {
                name: ident!("foo"),
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
    fn test_to_sql() {
        assert_eq!(
            vec!["topics[2] as a", "abi_bytes(abi_dynamic(data, 0)) as b"],
            {
                let mut param = parse("foo(int indexed a, bytes b)").unwrap();
                param.find(ident!("foo", "a")).unwrap().select();
                param.find(ident!("foo", "b")).unwrap().select();
                param.to_sql("data")
            }
        );
        assert_eq!(vec!["abi_fixed_bytes(data, 0, 32) as a"], {
            let mut param = parse("(int a ) foo").unwrap();
            param.find(ident!("foo", "a")).unwrap().select();
            param.to_sql("data")
        });
        assert_eq!(
            vec!["abi_fixed_bytes(abi_dynamic(data, 0), 32, 32) as c"],
            {
                let mut param = parse("((bytes b, int c) a) foo").unwrap();
                param.find(ident!("foo", "a", "c")).unwrap().select();
                param.to_sql("data")
            }
        );
        assert_eq!(
            vec!["abi_fixed_bytes(abi_fixed_bytes(data, 0, 32), 0, 32) as d"],
            {
                let mut param = parse("((bytes b) a, (uint d) c) foo").unwrap();
                param.find(ident!("foo", "c", "d")).unwrap().select();
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
                    "with data as (select {}) select abi_uint_array(b) from data",
                    param.to_sql("$1")[0]
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
                    "with data as (select {}) select abi_uint_array(b) from data",
                    param.to_sql("$1")[0]
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
