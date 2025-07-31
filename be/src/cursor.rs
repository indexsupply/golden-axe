use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use eyre::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::api;

#[derive(Clone, Debug, Default)]
pub struct Cursor(HashMap<u64, Option<u64>>);

impl Cursor {
    pub fn new(chain: u64, block_num: Option<u64>) -> Self {
        let mut map = HashMap::new();
        map.insert(chain, block_num);
        Cursor(map)
    }

    pub fn add_chains(&mut self, chains: &HashSet<u64>) {
        chains.iter().for_each(|&c| {
            self.0.entry(c).or_insert(None);
        })
    }

    pub fn contains(&self, chain: u64) -> bool {
        self.0.keys().any(|c| *c == chain)
    }

    pub fn chains(&self) -> Vec<u64> {
        self.0.keys().sorted().cloned().collect()
    }

    pub fn chain(&self) -> u64 {
        match self.0.keys().next() {
            Some(c) => *c,
            None => 0,
        }
    }

    pub fn set_block_height(&mut self, chain: u64, n: u64) {
        self.0.insert(chain, Some(n));
    }

    pub fn to_sql(&self, col_name: &str) -> String {
        let predicates = self
            .0
            .iter()
            .sorted_by_key(|(chain, _)| *chain)
            .map(|(chain, block_num)| match block_num {
                Some(n) => format!("(chain = {chain} and {col_name} >= {n})"),
                None => format!("chain = {chain}"),
            })
            .collect::<Vec<_>>();
        if predicates.len() == 1 {
            predicates[0].clone()
        } else {
            format!("({})", predicates.join(" or "))
        }
    }
}

impl FromStr for Cursor {
    type Err = api::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let numbers = s
            .split("-")
            .map(|str| str.parse::<u64>())
            .collect::<Result<Vec<_>, _>>()
            .wrap_err("cursor must be - separated numbers")?;
        if numbers.len() % 2 != 0 {
            return Err(api::Error::User(String::from("cursor must be even length")));
        }
        let mut cursor = Cursor::default();
        for pair in numbers.chunks_exact(2) {
            cursor.set_block_height(pair[0], pair[1]);
        }
        Ok(cursor)
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, (k, v)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, "-")?;
            }
            match v {
                Some(val) => write!(f, "{k}-{val}")?,
                None => write!(f, "{k}-0")?,
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Cursor::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Serialize for Cursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut pairs: Vec<u64> = Vec::with_capacity(self.0.len() * 2);
        for (k, v) in &self.0 {
            pairs.push(*k);
            pairs.push(v.unwrap_or(0));
        }
        let encoded = pairs
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join("-");
        serializer.serialize_str(&encoded)
    }
}
