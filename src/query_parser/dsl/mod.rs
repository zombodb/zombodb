use crate::query_parser::ast::{ComparisonOpcode, Expr, QualifiedField, Term};
use pgx::*;
use serde_json::json;
use std::collections::HashSet;

pub mod path_finder;

#[pg_extern]
fn dump_query(index: PgRelation, query: &str) -> JsonB {
    let mut used_fields = HashSet::new();
    let query =
        Expr::from_str(&index, "_zdb_all", query, &mut used_fields).expect("failed to parse query");

    JsonB(expr_to_dsl(&query))
}

#[pg_extern]
fn debug_query(index: PgRelation, query: &str) -> String {
    let mut used_fields = HashSet::new();
    let query =
        Expr::from_str(&index, "_zdb_all", query, &mut used_fields).expect("failed to parse query");

    let mut tree = format!("{:#?}", query);
    tree = tree.replace("\n", "\n   ");
    format!(
        "Normalized Query:\n   {}\nUsed Fields:\n   {:?}\nSyntaxTree:\n   {}",
        query, used_fields, tree
    )
}

pub fn expr_to_dsl(expr: &Expr) -> serde_json::Value {
    match expr {
        Expr::WithList(v) => {
            let _dsl: Vec<serde_json::Value> = v.iter().map(|v| expr_to_dsl(v)).collect();
            panic!("WITH clauses not supported yet")
        }
        Expr::AndList(v) => {
            let dsl: Vec<serde_json::Value> = v.iter().map(|v| expr_to_dsl(v)).collect();
            json! { { "bool": { "must": dsl } } }
        }
        Expr::OrList(v) => {
            let dsl: Vec<serde_json::Value> = v.iter().map(|v| expr_to_dsl(v)).collect();
            json! { { "bool": { "should": dsl } } }
        }
        Expr::Not(r) => {
            let r = expr_to_dsl(r.as_ref());
            json! { { "bool": { "must_not": [r] } } }
        }
        Expr::Contains(f, t) => term_to_dsl(f, t, ComparisonOpcode::Contains),
        Expr::Regex(f, t) => term_to_dsl(f, t, ComparisonOpcode::Regex),

        _ => panic!("unsupported Expression: {:?}", expr),
    }
}

pub fn term_to_dsl(
    field: &QualifiedField,
    term: &Term,
    opcode: ComparisonOpcode,
) -> serde_json::Value {
    match opcode {
        ComparisonOpcode::Contains | ComparisonOpcode::Eq => eq(field, term),
        ComparisonOpcode::Regex => regex(field, term),

        ComparisonOpcode::DoesNotContain => {
            json! { { "bool": { "must_not": [ eq(field, term) ] } } }
        }

        ComparisonOpcode::Gt => {
            let (v, b) = range(term);
            json! { { "range": { field.field: { "gt": v, "boost": b }} } }
        }
        ComparisonOpcode::Lt => {
            let (v, b) = range(term);
            json! { { "range": { field.field: { "lt": v, "boost": b }} } }
        }
        ComparisonOpcode::Gte => {
            let (v, b) = range(term);
            json! { { "range": { field.field: { "gte": v, "boost": b }} } }
        }
        ComparisonOpcode::Lte => {
            let (v, b) = range(term);
            json! { { "range": { field.field: { "lte": v, "boost": b }} } }
        }
        ComparisonOpcode::Ne => json! { { "bool": { "must_not": [ eq(field, term) ] } } },
        // ComparisonOpcode::MoreLikeThis => {}
        // ComparisonOpcode::FuzzyLikeThis => {}
        _ => panic!("unsupported opcode {:?}", opcode),
    }
}

fn eq(field: &QualifiedField, term: &Term) -> serde_json::Value {
    match term {
        Term::Null => {
            json! { { "bool": { "must_not": [ { "exists": { "field": field.field } } ] } } }
        }
        Term::String(s, b) => json! { { "term": { field.field: { "value": s, "boost": b } } } },
        Term::Wildcard(w, b) => {
            json! { { "wildcard": { field.field: { "value": w, "boost": b } } } }
        }
        Term::Fuzzy(f, d, b) => {
            json! { { "fuzzy": { field.field: { "value": f, "prefix_length": d, "boost": b } } } }
        }
        Term::ParsedArray(v, _b) => {
            let mut strings = Vec::new();
            let mut clauses = Vec::new();

            for t in v {
                match t {
                    Term::String(s, _) => strings.push(s),
                    Term::Wildcard(_, _) | Term::Regex(_, _) | Term::Fuzzy(_, _, _) => {
                        clauses.push(eq(field, t))
                    }
                    _ => panic!("unsupported term in an array: {:?}", t),
                }
            }

            if !strings.is_empty() {
                clauses.push(json! { { "terms": { field.field: { "value": strings } } } })
            }

            if clauses.len() == 1 {
                clauses.pop().unwrap()
            } else {
                json! { { "bool": { "should": clauses } } }
            }
        }
        _ => panic!("unsupported Term: {:?}", term),
    }
}

fn range<'a>(term: &'a Term) -> (&'a str, &'a Option<f32>) {
    match term {
        Term::String(s, b) => (s, b),
        _ => panic!("invalid term type for a range"),
    }
}

fn regex(field: &QualifiedField, term: &Term) -> serde_json::Value {
    match term {
        Term::Regex(r, b) => json! { { "regex": { field.field: { "value": r, "boost": b }}}},
        _ => panic!("unsupported term for a regex query: {}", term),
    }
}
