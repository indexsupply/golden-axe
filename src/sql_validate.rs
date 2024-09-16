use alloy::{
    dyn_abi::{DynSolType, Specifier},
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
pub fn validate(user_query: &str, event_sigs: Vec<&str>) -> Result<RewrittenQuery, api::Error> {
    let mut reg = EventRegistry::new(event_sigs)?;
    let new_query = reg.validate(user_query)?;
    Ok(RewrittenQuery {
        new_query,
        selections: reg.selections(),
    })
}

pub struct RewrittenQuery {
    pub new_query: String,
    pub selections: Vec<Selection>,
}

#[derive(Debug)]
pub struct Selection {
    pub event: Event,
    pub alias: HashSet<String>,
    pub user_event_name: String,
    pub fields: HashSet<String>,
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
        let unquoted = field_name.replace('"', "");
        self.event
            .inputs
            .iter()
            .find(|inp| inp.name == unquoted)
            .cloned()
    }
}

#[derive(Debug)]
struct EventRegistry {
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

fn left_pad(vec: Vec<u8>) -> Vec<u8> {
    let mut padded = vec![0u8; 32 - vec.len()];
    padded.extend(vec);
    padded
}

pub const METADATA: [&str; 4] = ["address", "block_num", "log_idx", "tx_hash"];

trait ExprExt {
    fn is_metadata(&self) -> bool;
}

impl ExprExt for ast::Expr {
    fn is_metadata(&self) -> bool {
        match self {
            ast::Expr::Identifier(ident) => METADATA.contains(&ident.to_string().as_str()),
            ast::Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                METADATA.contains(&idents[1].to_string().as_str())
            }
            _ => false,
        }
    }
}

impl EventRegistry {
    fn new(event_sigs: Vec<&str>) -> Result<EventRegistry, api::Error> {
        let mut events = HashMap::new();
        let cleaned_event_sigs: Vec<&str> = event_sigs
            .into_iter()
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
                    alias: HashSet::new(),
                    user_event_name: String::new(),
                    fields: HashSet::new(),
                },
            );
        }
        Ok(EventRegistry { events })
    }

    fn selections(self) -> Vec<Selection> {
        self.events
            .into_values()
            .filter(|s| !s.fields.is_empty())
            .sorted_by_key(|s| s.user_event_name.to_string())
            .collect()
    }

    fn selection(&mut self, table_name: &str) -> Result<&mut Selection, api::Error> {
        let all_events = self.events.keys().join(",");
        for (event_name, selection) in self.events.iter_mut() {
            if event_name == table_name {
                return Ok(selection);
            }
            if selection.alias.contains(table_name) {
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

    fn select_field(&mut self, field_name: &str) -> Result<Option<EventParam>, api::Error> {
        let mut selection: Vec<&mut Selection> = self
            .events
            .values_mut()
            .filter(|s| s.get_field(field_name).is_some())
            .collect();
        match selection.len() {
            0 => {
                if METADATA.contains(&field_name) {
                    self.events.values_mut().for_each(|s| {
                        s.fields.insert(field_name.to_string());
                    });
                    return Ok(None);
                }
                Err(api::Error::User(format!(
                    r#"Unable to find an event which contains the field: '{}'"#,
                    field_name
                )))
            }
            1 => {
                let event = selection.first_mut().unwrap();
                event.fields.insert(field_name.to_string());
                Ok(event.get_field(field_name))
            }
            _ => Err(api::Error::User(format!(
                "multiple events contain field: {}",
                field_name
            ))),
        }
    }

    fn abi_decode_expr(&mut self, expr: ast::Expr) -> Result<ast::ExprWithAlias, api::Error> {
        let (alias, param) = match &expr {
            ast::Expr::Identifier(ident) => (ident.clone(), self.select_field(&ident.to_string())?),
            ast::Expr::CompoundIdentifier(idents) => (
                idents[1].clone(),
                self.select_event_field(&idents[0].to_string(), &idents[1].to_string())?,
            ),
            ast::Expr::Function(f) => match &f.args {
                ast::FunctionArguments::None => return no!("empty function args"),
                ast::FunctionArguments::Subquery(_) => return no!("subqueries in function args"),
                ast::FunctionArguments::List(fargs) => {
                    if fargs.args.len() > 1 {
                        return no!("multiple function args");
                    }
                    match fargs.args.first().unwrap().clone() {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner)) => {
                            let wrapped = self.abi_decode_expr(inner)?;
                            return Ok(wrap_function(
                                None,
                                f.name.0.first().unwrap().clone(),
                                wrapped.expr,
                            ));
                        }
                        _ => return no!("named function args"),
                    }
                }
            },
            _ => return no!("wrap non-ident or non-function"),
        };
        let param_type = match param {
            Some(p) => p.resolve().wrap_err("decoding param abi")?,
            None => {
                return Err(api::Error::User(format!("unable to decode: {:?}", param)));
            }
        };
        match param_type {
            DynSolType::Bool => Ok(wrap_function(
                Some(alias),
                ast::Ident::new("abi_bool"),
                expr.clone(),
            )),
            DynSolType::Bytes => Ok(ast::ExprWithAlias {
                alias: None,
                expr: expr.clone(),
            }),
            DynSolType::FixedBytes(_) => Ok(ast::ExprWithAlias {
                alias: None,
                expr: expr.clone(),
            }),
            DynSolType::String => Ok(ast::ExprWithAlias {
                alias: None,
                expr: expr.clone(),
            }),
            DynSolType::Address => Ok(wrap_function(
                Some(alias),
                ast::Ident::new("abi_address"),
                expr.clone(),
            )),
            DynSolType::Int(_) => Ok(wrap_function(
                Some(alias),
                ast::Ident::new("abi_int"),
                expr.clone(),
            )),
            DynSolType::Uint(_) => Ok(wrap_function(
                Some(alias),
                ast::Ident::new("abi_uint"),
                expr.clone(),
            )),
            DynSolType::Array(arr) => match arr.as_ref() {
                DynSolType::Uint(_) => Ok(wrap_function(
                    Some(alias),
                    ast::Ident::new("abi_uint_array"),
                    expr.clone(),
                )),
                DynSolType::Int(_) => Ok(wrap_function(
                    Some(alias),
                    ast::Ident::new("abi_int_array"),
                    expr.clone(),
                )),
                DynSolType::FixedBytes(_) => Ok(wrap_function_arg(
                    Some(alias),
                    ast::Ident::new("abi_fixed_bytes_array"),
                    expr.clone(),
                    number_arg(32),
                )),
                DynSolType::Tuple(_) => Ok(ast::ExprWithAlias {
                    alias: None,
                    expr: expr.clone(),
                }),
                _ => no!(arr.to_string()),
            },
            _ => no!(param_type.to_string()),
        }
    }

    // We rewrite select items to preform last-mile abi decoding.
    // The decoding that happens within the log loading CTE will keep
    // data in 32byte padded format. It is in the rewritten user query
    // that we convert to the ABI type. IE turn a 32byte word into a
    // uint via the abi_uint function.
    fn rewrite_select_item(&mut self, item: &mut ast::SelectItem) -> Result<(), api::Error> {
        match item {
            ast::SelectItem::UnnamedExpr(expr) if !expr.is_metadata() => {
                let wrapped = self.abi_decode_expr(expr.clone())?;
                match wrapped.alias {
                    Some(alias) => {
                        *item = ast::SelectItem::ExprWithAlias {
                            alias,
                            expr: wrapped.expr,
                        };
                    }
                    None => *item = ast::SelectItem::UnnamedExpr(wrapped.expr),
                }
                Ok(())
            }
            ast::SelectItem::ExprWithAlias { expr, alias } if !expr.is_metadata() => {
                let wrapped = self.abi_decode_expr(expr.clone())?;
                *item = ast::SelectItem::ExprWithAlias {
                    alias: alias.clone(),
                    expr: wrapped.expr,
                };
                Ok(())
            }
            _ => Ok(()),
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
                hex::decode(str.replace(r#"\x"#, "")).wrap_err("decoding hex string")?
            }
            ast::Expr::Value(ast::Value::HexStringLiteral(str)) => {
                hex::decode(str).wrap_err("decoding hex string")?
            }
            ast::Expr::Value(ast::Value::Number(str, _)) => {
                let n = U256::from_str(str).wrap_err("unable to decode number")?;
                n.abi_encode()
            }
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
            DynSolType::Int(_) => {}
            DynSolType::Uint(_) => {
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                    r#"\x{}"#,
                    hex::encode(left_pad(data))
                )))
            }
            DynSolType::FixedBytes(_) => {}
            _ => {}
        };
        Ok(())
    }

    fn rewrite_binary_expr(
        &mut self,
        left: &mut Box<ast::Expr>,
        right: &mut Box<ast::Expr>,
    ) -> Result<(), api::Error> {
        match left.as_mut() {
            ast::Expr::Identifier(ident) => {
                let field_name = ident.to_string();
                match self.select_field(&field_name)? {
                    Some(param) => self.rewrite_literal(right, param.resolve().unwrap(), false),
                    None if field_name == "address" => {
                        self.rewrite_literal(right, DynSolType::Address, true)
                    }
                    _ => Ok(()),
                }
            }
            ast::Expr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    let event_name = idents[0].to_string();
                    let field_name = idents[1].to_string();
                    match self.select_event_field(&event_name, &field_name)? {
                        Some(param) => self.rewrite_literal(right, param.resolve().unwrap(), false),
                        None if field_name == "address" => {
                            self.rewrite_literal(right, DynSolType::Address, true)
                        }
                        _ => Ok(()),
                    }
                } else {
                    Err(api::Error::User(
                        "only 'event.field' format is supported".to_string(),
                    ))
                }
            }
            _ => Ok(()),
        }
    }

    fn validate(&mut self, user_query: &str) -> Result<String, api::Error> {
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
                    self.rewrite_select_item(projection_item)?;
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
            _ => no!(expr),
        }
    }

    fn validate_function(&mut self, function: &mut ast::Function) -> Result<(), api::Error> {
        let name = function.name.to_string().to_lowercase();
        const VALID_FUNCS: [&str; 11] = [
            "sum",
            "count",
            "b2i",
            "h2s",
            "abi_bool",
            "abi_fixed_bytes",
            "abi_address",
            "abi_uint",
            "abi_uint_array",
            "abi_int_array",
            "abi_fixed_bytes_array",
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

    fn validate_column(&mut self, id: &mut ast::Ident) -> Result<(), api::Error> {
        self.select_field(&id.to_string())?;
        Ok(())
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
                selection.alias.insert(alias.name.value.to_string());
                selection.user_event_name = name_parts[0].value.to_string();
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
                selection.user_event_name = name_parts[0].value.to_string();
                Ok(())
            }
            _ => no!(relation),
        }
    }
}
