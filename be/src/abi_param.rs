#![allow(dead_code)]
use std::collections::VecDeque;

use eyre::{eyre, Result};
use itertools::Itertools;

fn parse(input: &str) -> Result<Param> {
    Param::parse(&mut Token::lex(input)?)
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
    FixedArray(u16, Box<Kind>),
    Array(Box<Kind>),

    Bytes(Option<u16>),
    Uint(u16),
    Int(u16),
}

impl Kind {
    fn is_static(&self) -> bool {
        match &self {
            Kind::Tuple(fields) => fields.iter().all(Self::is_static),
            Kind::Array(_) => false,
            Kind::FixedArray(_, kind) => kind.is_static(),
            Kind::Bytes(Some(_)) => true,
            Kind::Bytes(None) => false,
            Kind::Uint(_) => true,
            Kind::Int(_) => true,
        }
    }

    fn size(&self) -> u16 {
        match &self {
            Kind::Tuple(fields) if self.is_static() => fields.iter().map(Self::size).sum(),
            Kind::FixedArray(size, kind) if kind.is_static() => size * kind.size(),
            Kind::FixedArray(_, _) => 32,
            Kind::Array(_) => 32,
            Kind::Tuple(_) => 32,
            Kind::Bytes(Some(size)) => *size,
            Kind::Bytes(None) => 32,
            Kind::Uint(size) => size / 8,
            Kind::Int(size) => size / 8,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Param {
    name: String,
    kind: Kind,
    components: Option<Vec<Param>>,
    selected: Option<bool>,
}

impl Param {
    fn new(name: &str, kind: Kind) -> Param {
        Param {
            kind,
            name: name.to_owned(),
            components: None,
            selected: None,
        }
    }

    fn from_components(name: &str, components: Vec<Param>) -> Param {
        Param {
            name: name.to_owned(),
            kind: Kind::Tuple(components.iter().map(|c| c.kind.clone()).collect()),
            components: Some(components),
            selected: None,
        }
    }

    fn parse(input: &mut VecDeque<Token>) -> Result<Param> {
        let mut inner = match input.pop_front() {
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
                        _ => return Err(eyre!("unhandled token: {:?}", token)),
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
                } else {
                    return Err(eyre!("not yet implemented"));
                }
            }
            None => return Err(eyre!("eof")),
            _ => return Err(eyre!("parse error")),
        };
        while let Some(Token::Array(size)) = input.front() {
            inner.kind = match size {
                Some(s) => Kind::FixedArray(*s, Box::new(inner.kind.clone())),
                None => Kind::Array(Box::new(inner.kind.clone())),
            };
            input.pop_front();
        }
        if let Some(Token::Word(word)) = input.front() {
            inner.name = word.clone();
            input.pop_front();
            Ok(inner)
        } else {
            Err(eyre!("parse error"))
        }
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
                        Kind::Array(_) => todo!(),
                        Kind::FixedArray(_, _) => todo!(),
                        Kind::Bytes(None) => {
                            result.push(format!("abi_bytes(abi_dynamic({}, {}))", inner, pos));
                        }
                        Kind::Bytes(Some(_)) | Kind::Uint(_) | Kind::Int(_) => {
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
    fn test_static() {
        assert!(Kind::Int(256).is_static());
        assert!(Kind::FixedArray(1, Box::new(Kind::Int(256))).is_static());
        assert!(!Kind::Array(Box::new(Kind::Int(256))).is_static());
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
            Param::parse(&mut Token::lex("(int bar) foo").unwrap()).unwrap(),
            Param::from_components("foo", vec![Param::new("bar", Kind::Int(256))])
        );
        assert_eq!(
            Param::parse(&mut Token::lex("(int[] bar) foo").unwrap()).unwrap(),
            Param::from_components(
                "foo",
                vec![Param::new("bar", Kind::Array(Box::new(Kind::Int(256))))]
            )
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
}
