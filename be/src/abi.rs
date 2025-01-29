use std::collections::VecDeque;

use alloy::{
    hex::ToHexExt,
    primitives::{keccak256, FixedBytes, U256, U64},
};
use eyre::{eyre, OptionExt, Result};
use itertools::Itertools;
use sqlparser::ast::Ident;

use crate::s256;

#[derive(Debug)]
pub struct Event {
    pub name: Ident,
    pub fields: Parameter,
}

impl Event {
    pub fn parse(input: &str) -> Result<Event> {
        let input = input.trim();
        let input = input.strip_prefix("event").unwrap_or(input);
        let (name, tuple_desc) = match input.find('(') {
            Some(index) => (&input[..index], &input[index..]),
            None => (input, ""),
        };
        Ok(Event {
            name: Ident::new(name),
            fields: Token::parse(&mut Token::lex(tuple_desc)?)?,
        })
    }

    pub fn has_field(&self, id: &Ident) -> bool {
        match &self.fields {
            Parameter::Tuple { components, .. } => components.iter().any(|c| c.name() == *id),
            _ => false,
        }
    }

    pub fn topics_sql(&self) -> Vec<(Ident, String)> {
        todo!()
    }

    pub fn data_sql(&self) -> Vec<(Ident, String)> {
        todo!()
    }

    pub fn sighash(&self) -> FixedBytes<32> {
        todo!()
    }
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

    fn parse(input: &mut VecDeque<Token>) -> Result<Parameter> {
        let mut parameter = match input.pop_front() {
            Some(Token::OpenParen) => {
                let mut components = Vec::new();
                while let Some(token) = input.front() {
                    match token {
                        Token::OpenParen | Token::Word(_) => {
                            components.push(Self::parse(input)?);
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
                Parameter::Tuple {
                    name: None,
                    indexed: None,
                    components,
                }
            }
            Some(Token::Word(type_desc)) => {
                if let Some(bits) = type_desc.strip_prefix("int") {
                    Parameter::Int {
                        name: None,
                        indexed: None,
                        bits: bits.parse().unwrap_or(256),
                    }
                } else if let Some(bits) = type_desc.strip_prefix("uint") {
                    Parameter::Uint {
                        name: None,
                        indexed: None,
                        bits: bits.parse().unwrap_or(256),
                    }
                } else if let Some(bytes) = type_desc.strip_prefix("bytes") {
                    Parameter::Bytes {
                        name: None,
                        indexed: None,
                        size: bytes.parse().ok(),
                    }
                } else if type_desc == "address" {
                    Parameter::Address {
                        name: None,
                        indexed: None,
                    }
                } else if type_desc == "bool" {
                    Parameter::Bool {
                        name: None,
                        indexed: None,
                    }
                } else if type_desc == "string" {
                    Parameter::String {
                        name: None,
                        indexed: None,
                    }
                } else {
                    return Err(eyre!("{} not yet implemented", type_desc));
                }
            }
            None => return Err(eyre!("eof")),
            _ => return Err(eyre!("expected '(' or word")),
        };
        while let Some(Token::Array(size)) = input.front() {
            parameter = Parameter::Array {
                name: None,
                indexed: None,
                length: *size,
                element: Box::new(parameter.clone()),
            };
            input.pop_front();
        }
        if let Some(Token::Word(word)) = input.front() {
            if word == "indexed" {
                parameter.set_indexed();
                input.pop_front();
            }
        }
        if let Some(Token::Word(word)) = input.front() {
            parameter.set_name(word);
            input.pop_front();
        }
        Ok(parameter)
    }
}

macro_rules! get_field {
    ($param:expr, $field:ident) => {
        match $param {
            Parameter::Tuple { $field, .. }
            | Parameter::Array { $field, .. }
            | Parameter::Address { $field, .. }
            | Parameter::Bool { $field, .. }
            | Parameter::Bytes { $field, .. }
            | Parameter::String { $field, .. }
            | Parameter::Int { $field, .. }
            | Parameter::Uint { $field, .. } => $field,
        }
    };
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Parameter {
    Tuple {
        name: Option<Ident>,
        indexed: Option<bool>,
        components: Vec<Parameter>,
    },
    Array {
        name: Option<Ident>,
        indexed: Option<bool>,
        length: Option<usize>,
        element: Box<Parameter>,
    },
    Address {
        name: Option<Ident>,
        indexed: Option<bool>,
    },
    Bool {
        name: Option<Ident>,
        indexed: Option<bool>,
    },
    Bytes {
        name: Option<Ident>,
        indexed: Option<bool>,
        size: Option<usize>,
    },
    String {
        name: Option<Ident>,
        indexed: Option<bool>,
    },
    Int {
        name: Option<Ident>,
        indexed: Option<bool>,
        bits: usize,
    },
    Uint {
        name: Option<Ident>,
        indexed: Option<bool>,
        bits: usize,
    },
}

impl Parameter {
    pub fn parse(input: &str) -> Result<Parameter> {
        Token::parse(&mut Token::lex(input.trim())?)
    }

    fn name(&self) -> Ident {
        get_field!(self, name)
            .as_ref()
            .map_or_else(|| Ident::new(""), |n| n.clone())
    }

    fn set_name(&mut self, name: &str) {
        *get_field!(self, name) = Some(Ident::new(name));
    }

    fn indexed(&self) -> bool {
        get_field!(self, indexed).unwrap_or(false)
    }

    fn set_indexed(&mut self) {
        *get_field!(self, indexed) = Some(true);
    }

    /// number of evm words occupied by the kind
    /// will always be a multiple of 32
    /// most of the time it _is_ 32 unless there
    /// is a static array or static tuple
    fn size(&self) -> usize {
        match self {
            Parameter::Tuple { components, .. } if self.is_static() => {
                components.iter().map(Self::size).sum()
            }
            Parameter::Array {
                length: Some(length),
                element,
                ..
            } if element.is_static() => 32 + length * element.size(),
            _ => 32,
        }
    }

    fn is_static(&self) -> bool {
        match self {
            Parameter::Tuple { components, .. } => components.iter().all(Parameter::is_static),
            Parameter::Array {
                length: Some(_),
                element,
                ..
            } => element.is_static(),
            Parameter::Array { length: None, .. } => false,
            Parameter::Address { .. } => true,
            Parameter::Bool { .. } => true,
            Parameter::Bytes { size: None, .. } => false,
            Parameter::Bytes { size: Some(_), .. } => true,
            Parameter::Int { .. } => true,
            Parameter::Uint { .. } => true,
            Parameter::String { .. } => false,
        }
    }

    fn signature(&self) -> String {
        todo!()
    }

    pub fn topics_to_sql(&self) -> Vec<(Ident, String)> {
        if let Self::Tuple { components, .. } = self {
            components
                .iter()
                .enumerate()
                .filter(|(_, param)| param.indexed())
                .map(|(pos, param)| (param.name(), format!("topics[{}]", pos + 2)))
                .collect()
        } else {
            vec![]
        }
    }

    pub fn to_sql(&self, inner: &str) -> Vec<(Ident, String)> {
        match self {
            Parameter::Tuple { components, .. } => components
                .iter()
                .filter(|p| !p.indexed())
                .scan(0, |size_counter, param| {
                    let size = *size_counter;
                    *size_counter += param.size();
                    Some((size, param))
                })
                .map(|(pos, component)| match component {
                    Parameter::Tuple { .. } | Parameter::Array { .. } if component.is_static() => (
                        component.name(),
                        format!(
                            "abi2json(abi_fixed_bytes({}, {}, {}), '{}')",
                            inner,
                            pos,
                            component.size(),
                            component.signature()
                        ),
                    ),
                    Parameter::Tuple { .. } | Parameter::Array { .. } => (
                        component.name(),
                        format!(
                            "abi2json(abi_dynamic({}, {}), '{}')",
                            inner,
                            pos,
                            component.signature()
                        ),
                    ),
                    Parameter::Address { .. } => (
                        component.name(),
                        format!("abi_fixed_bytes({}, {}, {})", inner, pos, component.size()),
                    ),
                    _ => todo!(),
                })
                .collect(),
            _ => vec![],
        }
    }
}

impl std::fmt::Display for Parameter {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Tuple {
                name, components, ..
            } => {
                write!(f, "(")?;
                for (i, c) in components.iter().enumerate() {
                    c.fmt(f)?;
                    if !matches!(c, Self::Array { .. }) {
                        write!(f, " {}", c.name())?;
                    }
                    if i != components.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            Self::Array {
                name,
                length: Some(length),
                element,
                ..
            } => {
                element.fmt(f)?;
                if !matches!(element.as_ref(), Self::Array { .. }) {
                    write!(f, "[{}] {}", length, self.name())
                } else {
                    write!(f, "[{}]", length)
                }
            }
            Self::Array {
                name,
                length: None,
                element,
                ..
            } => {
                element.fmt(f)?;
                if !matches!(element.as_ref(), Self::Array { .. }) {
                    write!(f, "[] {}", self.name())
                } else {
                    write!(f, "[]")
                }
            }
            Self::Address { name, .. }
            | Self::Bool { name, .. }
            | Self::Bytes { name, .. }
            | Self::Int { name, .. }
            | Self::Uint { name, .. }
            | Self::String { name, .. } => self.fmt(f),
        }
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

pub fn to_json(input: &[u8], param: &Parameter) -> Result<serde_json::Value> {
    match param {
        Parameter::Array {
            element, length, ..
        } => Ok(serde_json::Value::Array(
            (0..length.unwrap_or(input.next_usize(0)?))
                .map(|i| {
                    if element.is_static() {
                        Ok(to_json(
                            input.get_static(i * element.size(), element.size())?,
                            element,
                        )?)
                    } else {
                        Ok(to_json((&input[32..]).skip_to(32 * i)?, element)?)
                    }
                })
                .collect::<Result<_>>()?,
        )),
        Parameter::Tuple { components, .. } => Ok(serde_json::Value::Object(
            components
                .iter()
                .filter(|p| !p.indexed())
                .scan(0, |offset, c| {
                    let i = *offset;
                    *offset += c.size();
                    Some((i, c))
                })
                .map(|(i, c)| {
                    let field = if c.is_static() {
                        to_json(input.get_static(i, c.size())?, c)?
                    } else {
                        to_json(input.skip_to(i)?, c)?
                    };
                    Ok((c.name().to_string(), field))
                })
                .collect::<Result<_>>()?,
        )),
        Parameter::Address { .. } => Ok(input.get_static(12, 20)?.encode_hex().into()),
        Parameter::Bool { .. } => Ok((input.get_static(31, 1)?[0] == 1).into()),
        Parameter::Bytes { size: None, .. } => Ok(input.get_dynamic(0)?.encode_hex().into()),
        Parameter::Bytes {
            size: Some(size), ..
        } => Ok(input.get_static(0, *size)?.encode_hex().into()),
        Parameter::String { .. } => Ok(String::from_utf8(input.get_dynamic(0)?.to_vec())?.into()),
        Parameter::Int { .. } => Ok(s256::Int::try_from_be_slice(input.get_static(0, 32)?)
            .ok_or_eyre("decoding i256")?
            .to_string()
            .into()),
        Parameter::Uint { .. } => Ok(U256::try_from_be_slice(input.get_static(0, 32)?)
            .ok_or_eyre("decoding u256")?
            .to_string()
            .into()),
    }
}

#[cfg(test)]
mod json_tests {
    use crate::abi::Event;

    use super::to_json;
    use alloy::hex;
    use assert_json_diff::assert_json_eq;

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
        let param = Event::parse("Foo((string b, string[] c, string[] d)[] a)").unwrap();
        assert_json_eq!(
            to_json(&data, &param.fields).unwrap(),
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

    use crate::abi::{Event, Parameter};

    use super::Token;
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
    fn test_sighash() {
        assert_eq!(
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            Event::parse("Transfer(address from, address to, uint256 value)")
                .unwrap()
                .sighash()
                .encode_hex()
        );
        assert_eq!(
            "8dbb3a9672eebfd3773e72dd9c102393436816d832c7ba9e1e1ac8fcadcac7a9",
            Event::parse(
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
            Event::parse(
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
    fn test_static() {
        assert!(Parameter::Int {
            name: None,
            indexed: None,
            bits: 256
        }
        .is_static());
        assert!(Parameter::Array {
            name: None,
            indexed: None,
            length: Some(1),
            element: Box::new(Parameter::Int {
                name: None,
                indexed: None,
                bits: 256
            })
        }
        .is_static());
        assert!(!Parameter::Array {
            name: None,
            indexed: None,
            length: None,
            element: Box::new(Parameter::Int {
                name: None,
                indexed: None,
                bits: 256
            })
        }
        .is_static())
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
    fn test_parse() {
        assert_eq!(
            Token::parse(&mut Token::lex("(int a, (int b, int[])[])").unwrap()).unwrap(),
            Parameter::Tuple {
                name: None,
                indexed: None,
                components: vec![
                    Parameter::Int {
                        name: Some(ident!("a")),
                        indexed: None,
                        bits: 256,
                    },
                    Parameter::Array {
                        name: None,
                        indexed: None,
                        length: None,
                        element: Box::new(Parameter::Tuple {
                            name: None,
                            indexed: None,
                            components: vec![
                                Parameter::Int {
                                    name: Some(ident!("b")),
                                    indexed: None,
                                    bits: 256
                                },
                                Parameter::Array {
                                    name: None,
                                    indexed: None,
                                    length: None,
                                    element: Box::new(Parameter::Int {
                                        name: None,
                                        indexed: None,
                                        bits: 256
                                    })
                                }
                            ]
                        })
                    },
                ]
            }
        );
    }

    #[test]
    fn test_to_sql() {
        assert_eq!(
            vec![(ident!("b"), String::from("abi_bytes(abi_dynamic(data, 0))"))],
            Event::parse("foo(int indexed a, bytes b)")
                .unwrap()
                .data_sql()
        );
        assert_eq!(
            vec![(ident!("a"), String::from("abi_fixed_bytes(data, 0, 32)"))],
            Event::parse("foo(int a )").unwrap().data_sql()
        );
        assert_eq!(
            vec![(
                ident!("a"),
                String::from("abi2json(abi_dynamic(data, 0), '(bytes b, int256 c) a')")
            )],
            Event::parse("foo((bytes b, int c) a)").unwrap().data_sql()
        );
        assert_eq!(
            vec![(
                ident!("c"),
                String::from("abi2json(abi_fixed_bytes(data, 32, 32), '(uint256 d) c')")
            )],
            Event::parse("((bytes b) a, (uint d) c) foo")
                .unwrap()
                .data_sql()
        );
    }

    static SCHEMA: &str = include_str!("./sql/schema.sql");

    #[tokio::test]
    async fn test_static_array() {
        let pool = shared::pg::test::new(SCHEMA).await;
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
        let param = Parameter::parse("(uint[5] b) a").unwrap();
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
        let pool = shared::pg::test::new(SCHEMA).await;
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
        let param = Parameter::parse("(uint[] b) a").unwrap();
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
        let pool = shared::pg::test::new(SCHEMA).await;
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
        let event = Event::parse("IntentFinished(address indexed intentAddr, address indexed destinationAddr, bool indexed success,(uint256 toChainId, (address token, uint256 amount)[] bridgeTokenOutOptions, (address token, uint256 amount) finalCallToken, (address to, uint256 value, bytes data) finalCall, address escrow, address refundAddress, uint256 nonce) intent)").unwrap();
        let query = event.data_sql();
        let row = pg
            .query_one(
                &format!(
                    "with x as (select $1 as data) select {} from x",
                    &query[0].1
                ),
                &[&data],
            )
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
        let pool = shared::pg::test::new(SCHEMA).await;
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
        let event = Event::parse("Foo((string b, string[] c, string[] d)[] a)").unwrap();
        let query = event.data_sql();
        let row = pg
            .query_one(
                &format!(
                    "with x as (select $1 as data) select {} from x",
                    &query[0].1
                ),
                &[&data],
            )
            .await
            .expect("issue with query");
        let res: serde_json::Value = row.get(0);
        assert_json_eq!(
            res,
            serde_json::json!([{
                "b": "BB",
                "c": ["BB"],
                "d": ["BB"]
            }])
        )
    }
}
