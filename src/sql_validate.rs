use alloy::{
    dyn_abi::{DynSolType, Specifier},
    hex,
    json_abi::{Event, EventParam},
    primitives::U256,
    sol_types::SolValue,
};
use eyre::{Context, Result};
use itertools::Itertools;
use sqlparser::{ast, parser::Parser};
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
        selections: reg.selection(),
    })
}

pub struct RewrittenQuery {
    pub new_query: String,
    pub selections: Vec<Selection>,
}

#[derive(Debug)]
pub struct Selection {
    pub event: Event,
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

struct EventRegistry {
    events: HashMap<String, Selection>,
}

fn clean_ident(ident: &str) -> String {
    let uncased = ident.to_lowercase();
    uncased.replace('"', "")
}

pub const METADATA: [&str; 4] = ["block_num", "tx_hash", "address", "log_idx"];

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
                    user_event_name: String::new(),
                    fields: HashSet::new(),
                },
            );
        }
        Ok(EventRegistry { events })
    }

    fn selection(self) -> Vec<Selection> {
        self.events
            .into_values()
            .filter(|s| !s.fields.is_empty())
            .sorted_by_key(|s| s.user_event_name.to_string())
            .collect()
    }

    fn set_user_event_name(&mut self, event_name: &str) -> Result<(), api::Error> {
        if let Some(selection) = self.events.get_mut(&clean_ident(event_name)) {
            selection.user_event_name = event_name.to_string();
            Ok(())
        } else {
            Err(api::Error::User(format!(
                r#"
                You are attempting to query '{}' but it isn't defined.
                Possible events to query are: '{}'
                "#,
                event_name,
                self.events.keys().join(",")
            )))
        }
    }

    fn select_event_field(
        &mut self,
        event_name: &str,
        field_name: &str,
    ) -> Result<Option<EventParam>, api::Error> {
        let event = self.events.get_mut(event_name).ok_or_else(|| {
            api::Error::User(format!(
                "unable to find event in supplied schemas: {}",
                event_name
            ))
        })?;
        match event.get_field(field_name) {
            None => Err(api::Error::User(format!(
                "event {} has no field named {}",
                event_name, field_name
            ))),
            Some(param) => {
                event.fields.insert(field_name.to_string());
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

    fn rewrite_select_item(&mut self, item: &mut ast::SelectItem) -> Result<(), api::Error> {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(expr) => (expr, None),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.clone())),
            _ => return no!("wild card select items"),
        };
        let (alias_name, param) = match expr {
            ast::Expr::Identifier(ident) => {
                (ident.to_string(), self.select_field(&ident.to_string())?)
            }
            ast::Expr::CompoundIdentifier(idents) => {
                let event_name = idents[0].to_string();
                let field_name = idents[1].to_string();
                (
                    format!("{}.{}", event_name, field_name),
                    self.select_event_field(&event_name, &field_name)?,
                )
            }
            _ => return Ok(()),
        };
        let param = match param {
            None => return Ok(()),
            Some(p) if !p.indexed => return Ok(()),
            Some(p) => p,
        };
        let function_name = match param.resolve().wrap_err("decoding param abi")? {
            DynSolType::Address => ast::Ident::new("abi_address"),
            DynSolType::Int(_) => ast::Ident::new("abi_int"),
            DynSolType::Uint(_) => ast::Ident::new("abi_uint"),
            _ => return Ok(()),
        };
        let old_expr = std::mem::replace(expr, ast::Expr::Value(ast::Value::Null));
        *item = ast::SelectItem::ExprWithAlias {
            alias: alias.unwrap_or(ast::Ident::new(alias_name)),
            expr: ast::Expr::Function(ast::Function {
                name: ast::ObjectName(vec![function_name]),
                args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        old_expr,
                    ))],
                    clauses: vec![],
                }),
                null_treatment: None,
                filter: None,
                over: None,
                within_group: vec![],
            }),
        };
        Ok(())
    }

    fn rewrite_literal(&mut self, expr: &mut ast::Expr, ty: DynSolType) -> Result<(), api::Error> {
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
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                    r#"\x000000000000000000000000{}"#,
                    hex::encode(data)
                )));
            }
            DynSolType::Int(_) => {}
            DynSolType::Uint(_) => {
                *expr = ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                    r#"\x{}"#,
                    hex::encode(data)
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
                    Some(param) if param.indexed => {
                        self.rewrite_literal(right, param.resolve().unwrap())
                    }
                    None if field_name == "address" => {
                        self.rewrite_literal(right, DynSolType::Address)
                    }
                    _ => Ok(()),
                }
            }
            ast::Expr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    let event_name = idents[0].to_string();
                    let field_name = idents[1].to_string();
                    match self.select_event_field(&event_name, &field_name)? {
                        Some(param) if param.indexed => {
                            self.rewrite_literal(right, param.resolve().unwrap())
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
                for oexpr in order_by {
                    self.validate_expression(&mut oexpr.expr)?;
                }
                self.validate_query_body(body)
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
                for table_with_join in from {
                    self.validate_table(table_with_join)?;
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
        const VALID_FUNCS: [&str; 7] = [
            "sum",
            "count",
            "b2i",
            "h2s",
            "abi_fixed_bytes",
            "abi_address",
            "abi_uint",
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

    fn validate_table(&mut self, tbl_with_joins: &ast::TableWithJoins) -> Result<(), api::Error> {
        if !tbl_with_joins.joins.is_empty() {
            return no!("joins");
        }
        match &tbl_with_joins.relation {
            ast::TableFactor::Table { with_hints: h, .. } if !h.is_empty() => no!("with_hints"),
            ast::TableFactor::Table { args: Some(_), .. } => no!("args"),
            ast::TableFactor::Table {
                version: Some(_), ..
            } => no!("version"),
            ast::TableFactor::Table {
                name: ast::ObjectName(name_parts),
                ..
            } => {
                if name_parts.len() != 1 {
                    return Err(api::Error::User(format!(
                        "table {} has multiple parts; only unqualified table names supported",
                        tbl_with_joins.relation
                    )));
                }
                self.set_user_event_name(&name_parts[0].value.to_string())
            }
            _ => no!(tbl_with_joins.relation),
        }
    }
}
