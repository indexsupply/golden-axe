use alloy::json_abi::Event;
use eyre::{eyre, Context, Result};
use itertools::Itertools;
use sqlparser::{
    ast::{self, Function},
    parser::Parser,
};
use std::collections::{HashMap, HashSet};

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
pub fn validate(user_query: &str, event_sigs: Vec<&str>) -> Result<Vec<Selection>, api::Error> {
    let mut reg = EventRegistry::new(event_sigs)?;
    reg.validate(dbg!(user_query))?;
    Ok(reg.selection())
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

    fn has_field(&self, field_name: &str) -> bool {
        if ["block_num", "tx_hash", "log_idx", "address"].contains(&field_name) {
            return true;
        }
        let unquoted = field_name.replace('"', "");
        self.event.inputs.iter().any(|inp| inp.name == unquoted)
    }
}

struct EventRegistry {
    events: HashMap<String, Selection>,
}

fn clean_ident(ident: &str) -> String {
    let uncased = ident.to_lowercase();
    uncased.replace('"', "")
}

impl EventRegistry {
    fn new(event_sigs: Vec<&str>) -> Result<EventRegistry, api::Error> {
        let mut events = HashMap::new();
        for sig in event_sigs {
            let event: Event = sig
                .parse()
                .wrap_err(eyre!("unable to parse event: {}", sig))?;
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
        self.events.into_values().collect()
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

    fn select_event_field(&mut self, event_name: &str, field_name: &str) -> Result<(), api::Error> {
        let event = self.events.get_mut(event_name).ok_or_else(|| {
            api::Error::User(format!(
                "unable to find event in supplied schemas: {}",
                event_name
            ))
        })?;
        if !event.has_field(field_name) {
            return Err(api::Error::User(format!(
                "event {} has no field named {}",
                event_name, field_name
            )));
        }
        event.fields.insert(field_name.to_string());
        Ok(())
    }

    fn select_field(&mut self, field_name: &str) -> Result<(), api::Error> {
        let mut selection: Vec<&mut Selection> = self
            .events
            .values_mut()
            .filter(|s| s.has_field(field_name))
            .collect();
        match selection.len() {
            1 => {
                selection
                    .first_mut()
                    .unwrap()
                    .fields
                    .insert(field_name.to_string());
                Ok(())
            }
            0 => Err(api::Error::User(format!(
                r#"
                Unable to find an event which contains the field: '{}'
                "#,
                field_name
            ))),
            _ => Err(api::Error::User(format!(
                "multiple events contain field: {}",
                field_name
            ))),
        }
    }

    fn validate(&mut self, user_query: &str) -> Result<(), api::Error> {
        let stmts =
            Parser::parse_sql(PG, user_query).map_err(|e| api::Error::User(e.to_string()))?;
        for stmt in stmts.iter() {
            match stmt {
                ast::Statement::Query(q) => self.validate_query(q),
                _ => Err(api::Error::User("select queries only".to_string())),
            }?;
        }
        Ok(())
    }

    fn validate_query(&mut self, query: &ast::Query) -> Result<(), api::Error> {
        match query {
            ast::Query { with: Some(_), .. } => no!("with"),
            ast::Query { locks, .. } if !locks.is_empty() => no!("for update"),
            ast::Query { body, .. } => self.validate_query_body(body),
        }
    }

    fn validate_query_body(&mut self, body: &ast::SetExpr) -> Result<(), api::Error> {
        match body {
            ast::SetExpr::Select(select_query) => self.validate_select(select_query),
            _ => no!("invalid query body"),
        }
    }

    fn validate_select(&mut self, select: &ast::Select) -> Result<(), api::Error> {
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
                if let Some(ast::Distinct::On(exprs)) = distinct {
                    self.validate_expressions(exprs)?;
                }
                if let Some(expr) = selection {
                    self.validate_expression(expr)?;
                }
                if let ast::GroupByExpr::Expressions(exprs) = group_by {
                    self.validate_expressions(exprs)?;
                }
                for projection_item in projection.iter() {
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
                self.validate_expressions(sort_by)?;
                for table_with_join in from {
                    self.validate_table(table_with_join)?;
                }
                Ok(())
            }
        }
    }

    fn validate_expressions(&mut self, exprs: &[ast::Expr]) -> Result<(), api::Error> {
        for expr in exprs.iter() {
            self.validate_expression(expr)?;
        }
        Ok(())
    }

    fn validate_expression(&mut self, expr: &ast::Expr) -> Result<(), api::Error> {
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
            ast::Expr::Function(Function { name, .. }) => {
                if name.to_string().to_lowercase() == "sum" {
                    Ok(())
                } else {
                    no!(format!("function {}", name.to_string()))
                }
            }
            _ => no!(expr),
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
        self.select_event_field(&event_name, &field_name)
    }

    fn validate_column(&mut self, id: &ast::Ident) -> Result<(), api::Error> {
        self.select_field(&id.to_string())
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
