use alloy::{
    hex,
    primitives::{FixedBytes, U256},
};
use eyre::{Context, Result};
use itertools::Itertools;
use sqlparser::{
    ast::{self, Ident},
    parser::Parser,
};
use std::{collections::HashSet, ops::Deref, str::FromStr};

use crate::{
    abi::{self},
    api,
};

macro_rules! no {
    ($e:expr) => {
        Err(api::Error::User(format!("{} not supported", $e)))
    };
}

const PG: &sqlparser::dialect::PostgreSqlDialect = &sqlparser::dialect::PostgreSqlDialect {};

/// Parses the user supplied query into a SQL AST
/// and validates the query against the provided event signatures.
/// The SQL API implements onlny a subset of SQL so un-supported
/// SQL results in an error.
pub fn sql(
    chain: api::Chain,
    from: Option<u64>,
    user_query: &str,
    event_sigs: Vec<&str>,
) -> Result<String, api::Error> {
    let mut query = UserQuery::new(&event_sigs)?;
    let new_query = query.process(user_query)?;
    let query = [
        "with".to_string(),
        query
            .relations()
            .iter()
            .map(|rel| rel.to_sql(chain, from))
            .join(","),
        new_query.to_string(),
    ]
    .join(" ");
    Ok(query)
}

const METADATA: [&str; 7] = [
    "address",
    "block_num",
    "chain",
    "log_idx",
    "tx_hash",
    "topics",
    "data",
];

#[derive(Debug)]
struct Relation {
    event: Option<abi::Param>,
    // A single event can be referenced
    // multiple times eg multiple joins
    // on single table.
    table_alias: HashSet<Ident>,
    table_name: Ident,
    metadata: HashSet<Ident>,
}

impl Relation {
    fn named(&self, other: &Ident) -> bool {
        self.table_name.to_string().to_lowercase() == other.to_string().to_lowercase()
            || self.table_alias.contains(other)
    }

    /// Sets the field on the relation for SQL inclusion
    /// May return an abi::Param if the field is one
    /// It may select and _not_ return a Param in the case of METADATA
    fn field(&mut self, mut query: Vec<Ident>) -> Option<&mut abi::Param> {
        if let (Some(event), Some(other)) = (self.event.as_ref(), query.first()) {
            if self.named(other) {
                query[0] = event.name.clone();
            } else if other.value.ne(&event.name.value) {
                query.insert(0, event.name.clone());
            }
        }
        self.event.as_mut().and_then(|event| event.find(query))
    }

    fn to_sql(&self, chain: api::Chain, from: Option<u64>) -> String {
        let mut res: Vec<String> = Vec::new();
        res.push(format!("{} as not materialized (", self.table_name));
        res.push("select".to_string());
        let mut select_list = Vec::new();
        self.metadata.iter().sorted().for_each(|f| {
            select_list.push(f.to_string());
        });
        if let Some(param) = &self.event {
            for (ident, sql) in param.topics_to_sql() {
                select_list.push(format!("{} as {}", sql, ident));
            }
            for (ident, sql) in param.to_sql("data") {
                select_list.push(format!("{} as {}", sql, ident));
            }
        }
        res.push(select_list.join(","));
        res.push(format!("from logs where chain = {}", chain,));
        if let Some(topic) = self.event.as_ref().map(|e| e.sighash()) {
            res.push(format!(r#"and topics[1] = '\x{}'"#, hex::encode(topic)))
        }
        if let Some(n) = from {
            res.push(format!("and block_num >= {}", n))
        }
        res.push(")".to_string());
        res.join(" ")
    }
}

#[derive(Debug)]
struct UserQuery {
    relations: Vec<Relation>,
}

impl UserQuery {
    fn new(event_sigs: &[&str]) -> Result<UserQuery, api::Error> {
        let mut relations = vec![Relation {
            event: None,
            table_name: Ident::new("logs"),
            table_alias: HashSet::new(),
            metadata: HashSet::new(),
        }];
        for sig in event_sigs.iter().filter(|s| !s.is_empty()) {
            let event = abi::parse(sig)
                .map_err(|_| api::Error::User(format!("unable to parse event: {}", sig)))?;
            relations.push(Relation {
                table_name: event.name.clone(),
                table_alias: HashSet::new(),
                metadata: HashSet::new(),
                event: Some(event),
            });
        }
        Ok(UserQuery { relations })
    }

    fn process(&mut self, user_query: &str) -> Result<String, api::Error> {
        let mut stmts =
            Parser::parse_sql(PG, user_query).map_err(|e| api::Error::User(e.to_string()))?;
        if stmts.len() != 1 {
            return Err(api::Error::User(
                "query must be exactly 1 sql statement".to_string(),
            ));
        }
        let stmt = stmts.first_mut().unwrap();
        match stmt {
            ast::Statement::Query(q) => self.validate_query(q.as_mut()),
            _ => Err(api::Error::User("select queries only".to_string())),
        }?;
        Ok(stmt.to_string())
    }

    fn relations(self) -> Vec<Relation> {
        self.relations
            .into_iter()
            .filter(|rel| {
                rel.event.as_ref().is_some_and(|param| param.selected()) || !rel.metadata.is_empty()
            })
            .sorted_by_key(|s| s.table_name.to_string())
            .collect()
    }

    fn touch_relation(&mut self, name: &Ident, alias: Option<&Ident>) -> Result<(), api::Error> {
        let relations_debug_str = self
            .relations
            .iter()
            .map(|r| r.table_name.to_string().to_lowercase())
            .join(", ");
        let rel = self
            .relations
            .iter_mut()
            .find(|rel| rel.named(name))
            .ok_or_else(||
                api::Error::User(format!(
                    r#"You are attempting to query '{}' but it isn't defined. Possible events to query are: '{}'"#,
                    name, relations_debug_str,
                ))
            )?;
        rel.table_name = name.clone();
        alias.map(|a| rel.table_alias.insert(a.clone()));
        Ok(())
    }

    fn touch_metadata(&mut self, expr: &ast::Expr) {
        let query = expr.collect();
        if let (Some(event_name), Some(field_name)) = (query.first(), query.last()) {
            if METADATA.contains(&field_name.value.as_str()) {
                if self.relations.iter().any(|r| r.named(event_name)) {
                    let rel = self
                        .relations
                        .iter_mut()
                        .find(|rel| rel.named(event_name))
                        .expect("something went horribly wrong");
                    rel.metadata.insert(field_name.clone());
                } else if let Some(rel) = self
                    .relations
                    .iter_mut()
                    .sorted_by_key(|rel| rel.event.is_none())
                    .next()
                {
                    rel.metadata.insert(field_name.clone());
                }
            }
        }
    }

    fn touch_param(&mut self, expr: &ast::Expr) -> Option<&mut abi::Param> {
        let query = expr.collect();
        let possible_event_name = query.first()?;
        if self.relations.iter().any(|r| r.named(possible_event_name)) {
            self.relations
                .iter_mut()
                .find(|rel| rel.named(possible_event_name))
                .expect("something went horribly wrong")
                .field(query)
        } else {
            self.relations
                .iter_mut()
                .find_map(|rel| rel.field(query.clone()))
        }
    }

    fn abi_decode_expr(&mut self, expr: &ast::Expr) -> Option<ast::ExprWithAlias> {
        if let ast::Expr::UnaryOp { op, expr } = &expr {
            return match self.abi_decode_expr(expr) {
                Some(expr) => Some(ast::ExprWithAlias {
                    alias: None,
                    expr: ast::Expr::UnaryOp {
                        op: *op,
                        expr: Box::new(expr.expr),
                    },
                }),
                None => None,
            };
        }
        if let ast::Expr::BinaryOp { left, op, right } = &expr {
            return Some(ast::ExprWithAlias {
                alias: None,
                expr: ast::Expr::BinaryOp {
                    left: self.abi_decode_expr(left).map(|e| Box::new(e.expr))?,
                    right: self.abi_decode_expr(right).map(|e| Box::new(e.expr))?,
                    op: op.clone(),
                },
            });
        }
        if let ast::Expr::Value(_) = &expr {
            return Some(ast::ExprWithAlias {
                alias: None,
                expr: expr.clone(),
            });
        }
        if let ast::Expr::Function(f) = &expr {
            if let Some(expr) = extract_function_arg(f) {
                let wrapped = self.abi_decode_expr(&expr)?;
                return Some(wrap_function(
                    None,
                    f.name.0.first().unwrap().clone(),
                    wrapped.expr,
                ));
            }
        }
        if let ast::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } = &expr
        {
            return Some(ast::ExprWithAlias {
                alias: None,
                expr: ast::Expr::Case {
                    operand: operand.clone(),
                    conditions: conditions.clone(),
                    results: results
                        .iter()
                        .map(|e| self.abi_decode_expr(e).map(|e| e.expr))
                        .collect::<Option<_>>()?,
                    else_result: else_result
                        .as_ref()
                        .and_then(|expr| self.abi_decode_expr(expr))
                        .map(|rewritten| Box::new(rewritten.expr)),
                },
            });
        }
        let alias = expr.last();
        match &self.touch_param(expr)?.kind {
            abi::Kind::Bool => Some(wrap_function(
                alias,
                ast::Ident::new("abi_bool"),
                expr.clone(),
            )),
            abi::Kind::Address => Some(wrap_function(
                alias,
                ast::Ident::new("abi_address"),
                expr.clone(),
            )),
            abi::Kind::Int(_) => Some(wrap_function(
                alias,
                ast::Ident::new("abi_int"),
                expr.clone(),
            )),
            abi::Kind::Uint(_) => Some(wrap_function(
                alias,
                ast::Ident::new("abi_uint"),
                expr.clone(),
            )),
            abi::Kind::String => Some(wrap_function(
                alias,
                ast::Ident::new("abi_string"),
                expr.clone(),
            )),
            abi::Kind::Array(None, kind) => match kind.deref() {
                abi::Kind::Uint(_) => Some(wrap_function(
                    alias,
                    ast::Ident::new("abi_uint_array"),
                    expr.clone(),
                )),
                abi::Kind::Int(_) => Some(wrap_function(
                    alias,
                    ast::Ident::new("abi_int_array"),
                    expr.clone(),
                )),
                abi::Kind::Bytes(Some(_)) => Some(wrap_function_arg(
                    alias,
                    ast::Ident::new("abi_fixed_bytes_array"),
                    expr.clone(),
                    number_arg(32),
                )),
                _ => None,
            },
            _ => None,
        }
    }

    // We rewrite select items to preform last-mile abi decoding.
    // The decoding that happens within the log loading CTE will keep
    // data in 32byte padded format. It is in the rewritten user query
    // that we convert to the ABI type. IE turn a 32byte word into a
    // uint via the abi_uint function.
    fn rewrite_select_item(&mut self, item: &mut ast::SelectItem) {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                if let Some(rewritten) = self.abi_decode_expr(expr) {
                    match rewritten.alias {
                        Some(alias) => {
                            *item = ast::SelectItem::ExprWithAlias {
                                alias,
                                expr: rewritten.expr,
                            }
                        }
                        None => *item = ast::SelectItem::UnnamedExpr(rewritten.expr),
                    }
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(rewritten) = self.abi_decode_expr(expr) {
                    *item = ast::SelectItem::ExprWithAlias {
                        alias: alias.clone(),
                        expr: rewritten.expr,
                    };
                }
            }
            _ => {}
        }
    }

    fn rewrite_literal(
        &mut self,
        expr: &mut ast::Expr,
        kind: abi::Kind,
        compact: bool,
    ) -> Result<(), api::Error> {
        let data = match expr {
            ast::Expr::Value(ast::Value::SingleQuotedString(str)) => {
                match hex::decode(str.replace(r#"\x"#, "")) {
                    Ok(s) => s,
                    Err(_) => str.as_bytes().to_vec(),
                }
            }
            ast::Expr::Value(ast::Value::HexStringLiteral(str)) => {
                hex::decode(str).wrap_err("decoding hex string")?
            }
            ast::Expr::Value(ast::Value::Number(str, _)) => {
                let n = U256::from_str(str).wrap_err("unable to decode number")?;
                n.to_be_bytes_vec()
            }
            ast::Expr::Value(ast::Value::Boolean(b)) => {
                let mut res = FixedBytes::<32>::ZERO;
                if *b {
                    res[31] = 1;
                }
                res.to_vec()
            }
            _ => return Ok(()),
        };
        match kind {
            abi::Kind::Address => {
                let data = if compact {
                    format!(r#"\x{}"#, hex::encode(data))
                } else {
                    format!(r#"\x{}"#, hex::encode(left_pad(data)))
                };
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(data));
            }
            abi::Kind::Uint(_) => {
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                    r#"\x{}"#,
                    hex::encode(left_pad(data))
                )))
            }
            _ => {
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                    r#"\x{}"#,
                    hex::encode(data)
                )))
            }
        };
        Ok(())
    }

    fn rewrite_binary_expr(
        &mut self,
        left: &mut Box<ast::Expr>,
        right: &mut Box<ast::Expr>,
    ) -> Result<(), api::Error> {
        if let Some(param) = self.touch_param(left) {
            let kind = param.kind.clone();
            self.rewrite_literal(right, kind, false)?;
        }
        if left.last().map_or(false, |v| v.to_string() == "address") {
            self.rewrite_literal(right, abi::Kind::Address, true)?;
        }
        if left.last().map_or(false, |v| v.to_string() == "tx_hash") {
            self.rewrite_literal(right, abi::Kind::Bytes(Some(32)), false)?;
        }
        if left.last().map_or(false, |v| v.to_string() == "topics") {
            self.rewrite_literal(right, abi::Kind::Bytes(Some(32)), false)?;
        }
        Ok(())
    }

    fn validate_query(&mut self, query: &mut ast::Query) -> Result<(), api::Error> {
        match query {
            ast::Query { with: Some(_), .. } => no!("with"),
            ast::Query { locks, .. } if !locks.is_empty() => no!("for update"),
            ast::Query { body, order_by, .. } => {
                self.validate_query_body(body)?;
                for oexpr in order_by {
                    self.validate_expression(&mut oexpr.expr)?;
                }
                Ok(())
            }
        }
    }

    fn validate_query_body(&mut self, body: &mut ast::SetExpr) -> Result<(), api::Error> {
        match body {
            ast::SetExpr::Select(select_query) => self.validate_select(select_query.as_mut()),
            _ => no!("invalid query body"),
        }
    }

    fn validate_select(&mut self, select: &mut ast::Select) -> Result<(), api::Error> {
        match select {
            ast::Select { top: Some(_), .. } => no!("top"),
            ast::Select { into: Some(_), .. } => no!("into"),
            ast::Select { sort_by, .. } if !sort_by.is_empty() => no!("sort by"),
            ast::Select {
                having: Some(_), ..
            } => no!("having"),
            ast::Select {
                qualify: Some(_), ..
            } => no!("qualify"),
            ast::Select {
                value_table_mode: Some(_),
                ..
            } => no!("value_table_mode"),
            ast::Select {
                lateral_views: l, ..
            } if !l.is_empty() => no!("lateral"),
            ast::Select {
                distribute_by: d, ..
            } if !d.is_empty() => no!("distribute_by"),
            ast::Select { cluster_by: d, .. } if !d.is_empty() => no!("cluster_by"),
            ast::Select {
                named_window: w, ..
            } if !w.is_empty() => no!("named_window"),
            ast::Select { from, .. } if from.is_empty() => no!("empty tables"),
            ast::Select {
                distinct,
                projection,
                from,
                selection,
                group_by,
                ..
            } => {
                for table_with_join in from {
                    self.validate_table_with_joins(table_with_join)?;
                }
                if let Some(ast::Distinct::On(exprs)) = distinct.as_mut() {
                    self.validate_expressions(exprs)?;
                }
                if let Some(expr) = selection.as_mut() {
                    self.validate_expression(expr)?;
                }
                if let ast::GroupByExpr::Expressions(exprs) = group_by {
                    self.validate_expressions(exprs.as_mut())?;
                }
                for projection_item in projection.iter_mut() {
                    self.rewrite_select_item(projection_item);
                    match projection_item {
                        ast::SelectItem::UnnamedExpr(expr) => self.validate_expression(expr),
                        ast::SelectItem::ExprWithAlias { expr, alias: _ } => {
                            self.validate_expression(expr)
                        }
                        _ => {
                            no!(projection_item)
                        }
                    }?;
                }
                Ok(())
            }
        }
    }

    fn validate_expressions(&mut self, exprs: &mut [ast::Expr]) -> Result<(), api::Error> {
        for expr in exprs.iter_mut() {
            self.validate_expression(expr)?;
        }
        Ok(())
    }

    fn validate_expression(&mut self, expr: &mut ast::Expr) -> Result<(), api::Error> {
        match expr {
            ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) => {
                self.touch_metadata(expr);
                if let Some(param) = self.touch_param(expr) {
                    param.select();
                }
                Ok(())
            }
            ast::Expr::IsFalse(_) => Ok(()),
            ast::Expr::IsNotFalse(_) => Ok(()),
            ast::Expr::IsTrue(_) => Ok(()),
            ast::Expr::IsNotTrue(_) => Ok(()),
            ast::Expr::IsNull(_) => Ok(()),
            ast::Expr::IsNotNull(_) => Ok(()),
            ast::Expr::Ceil { expr, field: _ } => self.validate_expression(expr),
            ast::Expr::Floor { expr, field: _ } => self.validate_expression(expr),
            ast::Expr::Value(_) => Ok(()),
            ast::Expr::Exists { subquery, .. } => self.validate_query(subquery),
            ast::Expr::Subquery(subquery) => self.validate_query(subquery),
            ast::Expr::Tuple(exprs) => self.validate_expressions(exprs),
            ast::Expr::UnaryOp { expr, .. } => self.validate_expression(expr),
            ast::Expr::BinaryOp {
                left,
                right,
                op: ast::BinaryOperator::LongArrow | ast::BinaryOperator::Arrow,
                ..
            } => {
                self.validate_expression(left)?;
                self.validate_expression(right)
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.rewrite_binary_expr(left, right)?;
                self.validate_expression(left)?;
                self.validate_expression(right)
            }
            ast::Expr::InList { expr, list, .. } => {
                for e in list {
                    self.validate_expression(e)?;
                }
                self.validate_expression(expr)
            }
            ast::Expr::Subscript { expr, .. } => self.validate_expression(expr),
            ast::Expr::Substring { expr, .. } => self.validate_expression(expr),
            ast::Expr::Function(f) => self.validate_function(f),
            ast::Expr::Nested(expr) => self.validate_expression(expr),
            ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(e) = else_result {
                    self.validate_expression(e)?;
                }
                if let Some(e) = operand {
                    self.validate_expression(e)?;
                }
                self.validate_expressions(conditions)?;
                self.validate_expressions(results)
            }
            ast::Expr::Cast { .. } => Ok(()),
            _ => no!(expr),
        }
    }

    fn validate_function(&mut self, function: &mut ast::Function) -> Result<(), api::Error> {
        let name = function.name.to_string().to_lowercase();
        const VALID_FUNCS: [&str; 15] = [
            "min",
            "max",
            "sum",
            "count",
            "b2i",
            "h2s",
            "abi_bool",
            "abi_fixed_bytes",
            "abi_address",
            "abi_uint",
            "abi_int",
            "abi_uint_array",
            "abi_int_array",
            "abi_fixed_bytes_array",
            "abi_string",
        ];
        if !VALID_FUNCS.contains(&name.as_str()) {
            return no!(format!(r#"'{}' function"#, name));
        }
        match &mut function.args {
            ast::FunctionArguments::None => Ok(()),
            ast::FunctionArguments::Subquery(q) => self.validate_query(q.as_mut()),
            ast::FunctionArguments::List(l) => {
                for a in l.args.iter_mut() {
                    self.validate_function_arg(a)?;
                }
                Ok(())
            }
        }
    }

    fn validate_function_arg(&mut self, arg: &mut ast::FunctionArg) -> Result<(), api::Error> {
        match arg {
            ast::FunctionArg::Named { .. } => no!("named function args"),
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                self.validate_expression(expr)
            }
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::QualifiedWildcard(_)) => {
                no!("qualified wild card function args")
            }
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                no!("wild card function args")
            }
        }
    }

    fn validate_table_with_joins(
        &mut self,
        table: &mut ast::TableWithJoins,
    ) -> Result<(), api::Error> {
        self.validate_relation(&mut table.relation)?;
        for join in table.joins.iter_mut() {
            self.validate_relation(&mut join.relation)?;
            match &mut join.join_operator {
                ast::JoinOperator::Inner(c) => self.validate_join_constraint(c)?,
                ast::JoinOperator::LeftOuter(c) => self.validate_join_constraint(c)?,
                ast::JoinOperator::RightOuter(c) => self.validate_join_constraint(c)?,
                _ => return no!("must be inner, left outer, or right outer join"),
            };
        }
        Ok(())
    }

    fn validate_join_constraint(
        &mut self,
        constraint: &mut ast::JoinConstraint,
    ) -> Result<(), api::Error> {
        match constraint {
            ast::JoinConstraint::On(expr) => self.validate_expression(expr),
            _ => no!("must use ON join constraint"),
        }
    }

    fn validate_relation(&mut self, relation: &mut ast::TableFactor) -> Result<(), api::Error> {
        match relation {
            ast::TableFactor::Table { with_hints: h, .. } if !h.is_empty() => no!("with_hints"),
            ast::TableFactor::Table { args: Some(_), .. } => no!("args"),
            ast::TableFactor::Table {
                version: Some(_), ..
            } => no!("version"),
            ast::TableFactor::Table {
                name: ast::ObjectName(name_parts),
                alias: Some(alias),
                ..
            } => {
                if name_parts.len() != 1 {
                    return Err(api::Error::User(format!(
                        "table {} has multiple parts; only unqualified table names supported",
                        relation
                    )));
                }
                self.touch_relation(&name_parts[0], Some(&alias.name))
            }
            ast::TableFactor::Table {
                name: ast::ObjectName(name_parts),
                ..
            } => {
                if name_parts.len() != 1 {
                    return Err(api::Error::User(format!(
                        "table {} has multiple parts; only unqualified table names supported",
                        relation
                    )));
                }
                self.touch_relation(&name_parts[0], None)
            }
            _ => no!(relation),
        }
    }
}

fn left_pad(vec: Vec<u8>) -> Vec<u8> {
    let mut padded = vec![0u8; 32 - vec.len()];
    padded.extend(vec);
    padded
}

trait ExprExt {
    fn last(&self) -> Option<Ident>;
    fn collect(&self) -> Vec<Ident>;
}

impl ExprExt for ast::Expr {
    fn collect(&self) -> Vec<Ident> {
        match self {
            ast::Expr::Identifier(ident) => vec![ident.clone()],
            ast::Expr::CompoundIdentifier(idents) => idents.clone(),
            ast::Expr::Subscript { expr, .. } => expr.collect(),
            _ => vec![],
        }
    }
    fn last(&self) -> Option<ast::Ident> {
        match self {
            ast::Expr::Identifier(ident) => Some(ident.clone()),
            ast::Expr::CompoundIdentifier(idents) => idents.last().cloned(),
            ast::Expr::Subscript { expr, .. } => expr.last(),
            _ => None,
        }
    }
}

fn wrap_function(
    alias: Option<ast::Ident>,
    outer: ast::Ident,
    inner: ast::Expr,
) -> ast::ExprWithAlias {
    ast::ExprWithAlias {
        alias,
        expr: ast::Expr::Function(ast::Function {
            name: ast::ObjectName(vec![outer]),
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner))],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![],
        }),
    }
}

fn number_arg(i: u64) -> ast::FunctionArg {
    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(
        ast::Value::Number(i.to_string(), false),
    )))
}

fn expr_arg(expr: ast::Expr) -> ast::FunctionArg {
    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))
}

fn wrap_function_arg(
    alias: Option<ast::Ident>,
    outer: ast::Ident,
    inner: ast::Expr,
    arg: ast::FunctionArg,
) -> ast::ExprWithAlias {
    ast::ExprWithAlias {
        alias,
        expr: ast::Expr::Function(ast::Function {
            name: ast::ObjectName(vec![outer]),
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                args: vec![expr_arg(inner), arg],
                clauses: vec![],
                duplicate_treatment: None,
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![],
        }),
    }
}

fn extract_function_arg(function: &ast::Function) -> Option<ast::Expr> {
    if let ast::FunctionArguments::List(list) = &function.args {
        if list.args.is_empty() {
            return None;
        }
        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = &list.args[0] {
            return Some(expr.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const PG: &sqlparser::dialect::PostgreSqlDialect = &sqlparser::dialect::PostgreSqlDialect {};
    static SCHEMA: &str = include_str!("./sql/schema.sql");

    fn fmt_sql(sql: &str) -> Result<String> {
        let ast = sqlparser::parser::Parser::parse_sql(PG, sql)?;
        Ok(sqlformat::format(
            &ast[0].to_string(),
            &sqlformat::QueryParams::None,
            sqlformat::FormatOptions::default(),
        ))
    }

    async fn check_sql(event_sigs: Vec<&str>, user_query: &str, want: &str) {
        let got = sql(1.into(), None, user_query, event_sigs)
            .unwrap_or_else(|e| panic!("unable to create sql for:\n{} error: {:?}", user_query, e));
        let (got, want) = (
            fmt_sql(&got).unwrap_or_else(|_| panic!("unable to format got: {}", got)),
            fmt_sql(want).unwrap_or_else(|_| panic!("unable to format want: {}", want)),
        );
        if got.to_lowercase().ne(&want.to_lowercase()) {
            panic!("got:\n{}\n\nwant:\n{}\n", got, want);
        }
        let pool = shared::pg::test::new(SCHEMA).await;
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
    async fn test_nested_tuples() {
        check_sql(
            vec!["Foo(uint a, uint b, (uint d, bytes e) c)"],
            r#"select c->>'d' from foo"#,
            r#"
                with foo as not materialized (
                    select json_build_object(
                      'd',
                      abi_uint(abi_fixed_bytes(abi_dynamic(data, 64), 0, 32))::text,
                      'e',
                      encode(abi_bytes(abi_dynamic(abi_dynamic(data, 64), 32)), 'hex')
                    ) AS c
                    from logs
                    where chain = 1
                    and topics [1] = '\x851f2bcfcac86844a44298d8354312295b246183022d51c76398d898d87014fc'
                )
                select c->>'d' from foo
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
}
