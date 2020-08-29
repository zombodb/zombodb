use crate::access_method::options::ZDBIndexOptions;
use crate::query_parser::ast::{ComparisonOpcode, Expr, IndexLink, QualifiedField, Term};
use crate::query_parser::dsl::path_finder::PathFinder;
use crate::zdbquery::mvcc::build_visibility_clause;
use pgx::*;
use serde_json::json;
use std::collections::HashSet;

pub mod path_finder;

#[pg_extern(immutable, parallel_safe)]
fn dump_query(index: PgRelation, query: &str) -> String {
    let mut used_fields = HashSet::new();
    let query =
        Expr::from_str(&index, "_zdb_all", query, &mut used_fields).expect("failed to parse query");

    serde_json::to_string_pretty(&expr_to_dsl(&IndexLink::from_relation(&index), &query))
        .expect("failed to convert DSL to text")
}

#[pg_extern(immutable, parallel_safe)]
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

pub fn expr_to_dsl(root: &IndexLink, expr: &Expr) -> serde_json::Value {
    match expr {
        Expr::WithList(v) => {
            let _dsl: Vec<serde_json::Value> = v.iter().map(|v| expr_to_dsl(root, v)).collect();
            panic!("WITH clauses not supported yet")
        }
        Expr::AndList(v) => {
            let dsl: Vec<serde_json::Value> = v.iter().map(|v| expr_to_dsl(root, v)).collect();
            json! { { "bool": { "must": dsl } } }
        }
        Expr::OrList(v) => {
            let dsl: Vec<serde_json::Value> = v.iter().map(|v| expr_to_dsl(root, v)).collect();
            json! { { "bool": { "should": dsl } } }
        }
        Expr::Not(r) => {
            let r = expr_to_dsl(root, r.as_ref());
            json! { { "bool": { "must_not": [r] } } }
        }
        Expr::Contains(f, t) => term_to_dsl(f, t, ComparisonOpcode::Contains),
        Expr::Regex(f, t) => term_to_dsl(f, t, ComparisonOpcode::Regex),

        Expr::Linked(i, e) => {
            let mut pf = PathFinder::new(&root);
            IndexLink::from_zdb(&root.open_index().expect("failed to open index"))
                .into_iter()
                .for_each(|link| pf.push(link));

            // calculate the path from our root IndexLink
            let mut paths = pf
                .find_path(&root, &i.qualified_index)
                .expect(&format!("no index link path to {}", i.qualified_index));

            // bottom-up build a set of potentially nested "subselect" QueryDSL clauses
            // TODO:  need some kind of setting to indicate if the user has 'zdbjoin' installed
            //        on their ES cluster.  If they don't we could do something more manual here
            let mut current = expr_to_dsl(root, e.as_ref());
            while let Some(path) = paths.pop() {
                let target_index = path.open_index().expect("failed to open index");
                let index_options = ZDBIndexOptions::from(&target_index);

                let visibility_clause = build_visibility_clause(&index_options.index_name());
                // let query = current;
                let query = json! {
                    {
                        "bool": {
                            "must": [current],
                            "filter": [visibility_clause]
                        }
                    }
                };
                current = json! { {
                    "subselect": {
                        "index": index_options.index_name(),
                        "type": "_doc",
                        "left_fieldname": path.left_field,
                        "right_fieldname": path.right_field,
                        "query": query
                    }
                }}
            }

            current
        }

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
            json! { { "range": { field.field_name(): { "gt": v, "boost": b.unwrap_or(1.0) }} } }
        }
        ComparisonOpcode::Lt => {
            let (v, b) = range(term);
            json! { { "range": { field.field_name(): { "lt": v, "boost": b.unwrap_or(1.0) }} } }
        }
        ComparisonOpcode::Gte => {
            let (v, b) = range(term);
            json! { { "range": { field.field_name(): { "gte": v, "boost": b.unwrap_or(1.0) }} } }
        }
        ComparisonOpcode::Lte => {
            let (v, b) = range(term);
            json! { { "range": { field.field_name(): { "lte": v, "boost": b.unwrap_or(1.0) }} } }
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
            json! { { "bool": { "must_not": [ { "exists": { "field": field.field_name() } } ] } } }
        }
        Term::String(s, b) => {
            json! { { "term": { field.field_name(): { "value": s, "boost": b.unwrap_or(1.0) } } } }
        }
        Term::Wildcard(w, b) => {
            json! { { "wildcard": { field.field_name(): { "value": w, "boost": b.unwrap_or(1.0) } } } }
        }
        Term::Fuzzy(f, d, b) => {
            json! { { "fuzzy": { field.field_name(): { "value": f, "prefix_length": d, "boost": b.unwrap_or(1.0) } } } }
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
                clauses.push(json! { { "terms": { field.field_name(): strings } } })
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
        Term::Regex(r, b) => {
            json! { { "regex": { field.field_name(): { "value": r, "boost": b.unwrap_or(1.0) }}}}
        }
        _ => panic!("unsupported term for a regex query: {}", term),
    }
}
