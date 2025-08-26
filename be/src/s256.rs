use std::fmt;

use alloy::primitives::U256;
use eyre::{eyre, Result};
use tokio_postgres::types::{FromSql, Type};

pub enum Int {
    Pos(U256),
    Neg(U256),
}

impl fmt::Display for Int {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Int::Neg(n) => write!(f, "-{n}"),
            Int::Pos(n) => write!(f, "{n}"),
        }
    }
}

impl<'a> FromSql<'a> for Int {
    fn accepts(ty: &Type) -> bool {
        U256::accepts(ty)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::NUMERIC => {
                if raw.len() < 8 {
                    return Err(eyre!("numeric header too small").into());
                }
                let sign = i16::from_be_bytes(raw[4..6].try_into()?);
                if sign == 0x0000 {
                    let n = U256::from_sql(ty, raw)?;
                    Ok(Int::Pos(n))
                } else {
                    let mut copy: Vec<u8> = vec![0; raw.len()];
                    copy.copy_from_slice(raw);
                    copy[4..6].fill(0x00);
                    let n = U256::from_sql(ty, copy.as_ref())?;
                    Ok(Int::Neg(n))
                }
            }
            _ => Err(eyre!("unable to decode").into()),
        }
    }
}
