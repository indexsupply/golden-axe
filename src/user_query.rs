use alloy::{
    dyn_abi::{DynSolType, DynSolValue, Specifier},
    hex,
    json_abi::{Event, EventParam},
    primitives::U256,
    sol_types::SolValue,
};
use eyre::{Context, Result};
use itertools::Itertools;
use sqlparser::{
    ast::{self},
    parser::Parser,
};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::api;

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
pub fn process(user_query: &str, event_sigs: &[&str]) -> Result<RewrittenQuery, api::Error> {
    let mut query = UserQuery::new(event_sigs)?;
    let new_query = query.process(user_query)?;
    Ok(RewrittenQuery {
        new_query,
        selections: query.selections(),
    })
}

pub struct RewrittenQuery {
    pub new_query: String,
    pub selections: Vec<Selection>,
}

#[derive(Debug)]
pub struct Selection {
    pub event: Event,
    // A single event can be referenced
    // multiple times eg multiple joins
    // on single table.
    pub table_alias: HashSet<String>,
    pub table_name: String,
    pub fields: HashSet<String>,
    field_aliases: HashMap<String, String>,
}

impl Selection {
    pub fn selected_field(&self, field_name: &str) -> bool {
        self.fields
            .iter()
            .any(|field| clean_ident(field).eq(&clean_ident(field_name)))
    }

    pub fn quoted_field_name(&self, field_name: &str) -> Result<String, api::Error> {
        Ok(self
            .fields
            .iter()
            .find(|field| clean_ident(field) == clean_ident(field_name))
            .ok_or_else(|| api::Error::User(format!("unable to find field: {}", field_name)))?
            .to_string())
    }

    fn get_field(&self, field_name: &str) -> Option<EventParam> {
        let field_name = if let Some(name) = self.field_aliases.get(&clean_ident(field_name)) {
            name
        } else {
            field_name
        };
        self.event
            .inputs
            .iter()
            .find(|inp| clean_ident(&inp.name) == clean_ident(field_name))
            .cloned()
    }
}

#[derive(Debug)]
struct UserQuery {
    events: HashMap<String, Selection>,
}

fn clean_ident(ident: &str) -> String {
    let uncased = ident.to_lowercase();
    uncased.replace('"', "")
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

fn left_pad(vec: Vec<u8>) -> Vec<u8> {
    let mut padded = vec![0u8; 32 - vec.len()];
    padded.extend(vec);
    padded
}

pub const METADATA: [&str; 4] = ["address", "block_num", "log_idx", "tx_hash"];

trait ExprExt {
    fn last(&self) -> Option<ast::Ident>;
}

impl ExprExt for ast::Expr {
    fn last(&self) -> Option<ast::Ident> {
        match self {
            ast::Expr::Identifier(ident) => Some(ident.clone()),
            ast::Expr::CompoundIdentifier(idents) => idents.last().cloned(),
            _ => None,
        }
    }
}

impl UserQuery {
    fn new(event_sigs: &[&str]) -> Result<UserQuery, api::Error> {
        let mut events = HashMap::new();
        let cleaned_event_sigs: Vec<&str> = event_sigs
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect_vec();
        for sig in cleaned_event_sigs {
            let event: Event = sig
                .parse()
                .map_err(|_| api::Error::User(format!("unable to parse event: {}", sig)))?;
            events.insert(
                clean_ident(&event.name.to_string()),
                Selection {
                    event,
                    table_alias: HashSet::new(),
                    table_name: String::new(),
                    fields: HashSet::new(),
                    field_aliases: HashMap::new(),
                },
            );
        }
        Ok(UserQuery { events })
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

    fn selections(self) -> Vec<Selection> {
        self.events
            .into_values()
            .filter(|s| !s.fields.is_empty())
            .sorted_by_key(|s| s.table_name.to_string())
            .collect()
    }

    fn selection(&mut self, table_name: &str) -> Result<&mut Selection, api::Error> {
        let all_events = self.events.keys().join(",");
        for (event_name, selection) in self.events.iter_mut() {
            if event_name == table_name {
                return Ok(selection);
            }
            if selection.table_alias.contains(table_name) {
                return Ok(selection);
            }
        }
        Err(api::Error::User(format!(
            r#"
            You are attempting to query '{}' but it isn't defined.
            Possible events to query are: '{}'
            "#,
            table_name, all_events,
        )))
    }

    fn select_event_field(
        &mut self,
        event_name: &str,
        field_name: &str,
    ) -> Result<Option<EventParam>, api::Error> {
        let selection = self.selection(event_name)?;
        match selection.get_field(field_name) {
            None => {
                if METADATA.contains(&field_name) {
                    self.events.values_mut().for_each(|s| {
                        s.fields.insert(field_name.to_string());
                    });
                    Ok(None)
                } else {
                    Err(api::Error::User(format!(
                        "event {} has no field named {}",
                        event_name, field_name
                    )))
                }
            }
            Some(param) => {
                selection.fields.insert(field_name.to_string());
                Ok(Some(param))
            }
        }
    }

    // If the field_name matches an Event's field name
    // then we will save the field_name to our Selection's
    // field set. This will later be used to build the logs CTE.
    fn select_field(&mut self, field_name: &str) {
        if METADATA.contains(&field_name) {
            self.events.values_mut().for_each(|s| {
                s.fields.insert(field_name.to_string());
            });
            return;
        }
        let selection = self
            .events
            .values_mut()
            .find(|s| s.get_field(field_name).is_some());
        match selection {
            None => return,
            Some(s) => s.fields.insert(field_name.to_string()),
        };
    }

    fn event_param(&mut self, expr: &ast::Expr) -> Option<EventParam> {
        match expr {
            ast::Expr::Identifier(ident) => {
                for sel in self.events.values() {
                    if let Some(param) = sel.get_field(&ident.to_string()) {
                        return Some(param);
                    }
                }
                None
            }
            ast::Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                if let Ok(sel) = self.selection(&idents[0].to_string()) {
                    sel.get_field(&idents[1].to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn abi_decode_expr(&mut self, expr: &ast::Expr) -> Option<ast::ExprWithAlias> {
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
        let param = match self.event_param(expr) {
            None => return None,
            Some(p) => p.resolve().unwrap(),
        };
        let alias = expr.last();
        match param {
            DynSolType::Bool => Some(wrap_function(
                alias,
                ast::Ident::new("abi_bool"),
                expr.clone(),
            )),
            DynSolType::Address => Some(wrap_function(
                alias,
                ast::Ident::new("abi_address"),
                expr.clone(),
            )),
            DynSolType::Int(_) => Some(wrap_function(
                alias,
                ast::Ident::new("abi_int"),
                expr.clone(),
            )),
            DynSolType::Uint(_) => Some(wrap_function(
                alias,
                ast::Ident::new("abi_uint"),
                expr.clone(),
            )),
            DynSolType::String => Some(wrap_function(
                alias,
                ast::Ident::new("abi_string"),
                expr.clone(),
            )),
            DynSolType::Array(arr) => match arr.as_ref() {
                DynSolType::Uint(_) => Some(wrap_function(
                    alias,
                    ast::Ident::new("abi_uint_array"),
                    expr.clone(),
                )),
                DynSolType::Int(_) => Some(wrap_function(
                    alias,
                    ast::Ident::new("abi_int_array"),
                    expr.clone(),
                )),
                DynSolType::FixedBytes(_) => Some(wrap_function_arg(
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
        ty: DynSolType,
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
                n.abi_encode()
            }
            ast::Expr::Value(ast::Value::Boolean(b)) => DynSolValue::Bool(*b).abi_encode(),
            _ => return Ok(()),
        };
        match ty {
            DynSolType::Address => {
                let data = if compact {
                    format!(r#"\x{}"#, hex::encode(data))
                } else {
                    format!(r#"\x{}"#, hex::encode(left_pad(data)))
                };
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(data));
            }
            DynSolType::Uint(_) => {
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
        if let Some(param) = self.event_param(left) {
            self.rewrite_literal(right, param.resolve().unwrap(), false)?;
        }
        if left.last().map_or(false, |v| v.to_string() == "address") {
            self.rewrite_literal(right, DynSolType::Address, true)?;
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
                sort_by,
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
                self.validate_expressions(sort_by.as_mut())?;
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
            ast::Expr::Identifier(id) => self.validate_column(id),
            ast::Expr::CompoundIdentifier(ids) => self.validate_compound_column(ids),
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
            ast::Expr::Substring { expr, .. } => self.validate_expression(expr),
            ast::Expr::Function(f) => self.validate_function(f),
            ast::Expr::Nested(expr) => self.validate_expression(expr),
            _ => no!(expr),
        }
    }

    fn validate_column(&mut self, id: &mut ast::Ident) -> Result<(), api::Error> {
        self.select_field(&id.to_string());
        Ok(())
    }

    fn validate_compound_column(&mut self, id: &[ast::Ident]) -> Result<(), api::Error> {
        let (event_name, field_name) = match id.len() {
            3 => (id[0..2].iter().join("."), id[2].to_string()),
            2 => (id[0].to_string(), id[1].to_string()),
            _ => {
                return Err(api::Error::User(format!(
                    "compound column id must be of form: event.field got: {}",
                    id.iter().join(" ")
                )))
            }
        };
        self.select_event_field(&event_name, &field_name)?;
        Ok(())
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
                let selection = self.selection(&name_parts[0].value)?;
                selection.table_alias.insert(alias.name.value.to_string());
                selection.table_name = name_parts[0].value.to_string();
                Ok(())
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
                let selection = self.selection(&name_parts[0].value)?;
                selection.table_name = name_parts[0].value.to_string();
                Ok(())
            }
            _ => no!(relation),
        }
    }
}
