use std::collections::HashSet;

use pgx::*;
use serde_json::json;

use crate::access_method::options::ZDBIndexOptions;
use crate::gucs::ZDB_IGNORE_VISIBILITY;
use crate::query_parser::ast::{
    ComparisonOpcode, Expr, IndexLink, ProximityPart, ProximityTerm, QualifiedField, Term,
};
use crate::zdbquery::mvcc::build_visibility_clause;
use crate::zdbquery::ZDBQuery;

#[pg_extern(immutable, parallel_safe)]
fn dump_query(index: PgRelation, query: ZDBQuery) -> String {
    let query = query.prepare(&index);
    serde_json::to_string_pretty(query.query_dsl()).expect("failed to convert DSL to text")
}

#[pg_extern(immutable, parallel_safe)]
fn debug_query(
    index: PgRelation,
    query: &str,
) -> (
    name!(normalized_query, String),
    name!(used_fields, Vec<String>),
    name!(ast, String),
) {
    let mut used_fields = HashSet::new();
    let query = Expr::from_str(
        &index,
        "zdb_all",
        query,
        &IndexLink::from_zdb(&index),
        &mut used_fields,
    )
    .expect("failed to parse query");

    let tree = format!("{:#?}", query);

    (
        sqlformat::format(
            &format!("{}", query),
            &sqlformat::QueryParams::default(),
            sqlformat::FormatOptions::default(),
        )
        .replace(" :\"", ":\""),
        used_fields.into_iter().map(|v| v.into()).collect(),
        format!("{}", tree),
    )
}

pub fn expr_to_dsl(
    root: &IndexLink,
    index_links: &Vec<IndexLink>,
    expr: &Expr,
) -> serde_json::Value {
    match expr {
        Expr::Null => unreachable!(),

        Expr::Subselect(_, _) => unimplemented!("#subselect is not implemented yet"),
        Expr::Expand(link, e, _) => expr_to_dsl(link, index_links, e),

        Expr::WithList(_) => unreachable!("dsl conversion of Expr::WithList shouldn't happen"),
        Expr::AndList(v) => {
            let dsl: Vec<serde_json::Value> = v
                .iter()
                .map(|v| expr_to_dsl(root, index_links, v))
                .collect();
            json! { { "bool": { "must": dsl } } }
        }
        Expr::OrList(v) => {
            if v.len() == 1 {
                expr_to_dsl(root, index_links, v.get(0).unwrap())
            } else {
                let dsl: Vec<serde_json::Value> = v
                    .iter()
                    .map(|v| expr_to_dsl(root, index_links, v))
                    .collect();
                json! { { "bool": { "should": dsl } } }
            }
        }
        Expr::Not(r) => {
            let r = expr_to_dsl(root, index_links, r.as_ref());
            json! { { "bool": { "must_not": [r] } } }
        }
        Expr::Contains(f, t) | Expr::Eq(f, t) => term_to_dsl(f, t, ComparisonOpcode::Contains),
        Expr::DoesNotContain(f, t) | Expr::Ne(f, t) => {
            term_to_dsl(f, t, ComparisonOpcode::DoesNotContain)
        }
        Expr::Regex(f, t) => term_to_dsl(f, t, ComparisonOpcode::Regex),
        Expr::MoreLikeThis(_, _) => unimplemented!("more like this is not implemented yet"),
        Expr::FuzzyLikeThis(_, _) => unimplemented!("fuzzy like this is not implemented yet"),

        Expr::Gt(f, t) => term_to_dsl(f, t, ComparisonOpcode::Gt),
        Expr::Gte(f, t) => term_to_dsl(f, t, ComparisonOpcode::Gte),
        Expr::Lt(f, t) => term_to_dsl(f, t, ComparisonOpcode::Lt),
        Expr::Lte(f, t) => term_to_dsl(f, t, ComparisonOpcode::Lte),

        Expr::Json(json) => serde_json::from_str(&json).expect("failed to parse json expression"),

        Expr::Nested(p, e) => {
            let dsl = expr_to_dsl(root, index_links, e.as_ref());
            json! { { "nested": { "path": p, "query": dsl, "score_mode": "avg", "ignore_unmapped": false } } }
        }

        Expr::Linked(link, e) => {
            let mut query = expr_to_dsl(root, index_links, e.as_ref());
            if link.left_field.is_none() {
                query
            } else {
                let target_relation = link.open_index().unwrap_or_else(|e| {
                    panic!("failed to open index '{}': {}", link.qualified_index, e)
                });
                let index_options = ZDBIndexOptions::from(&target_relation);
                let es_index_name = index_options.index_name();

                query = if ZDB_IGNORE_VISIBILITY.get() {
                    query
                } else {
                    let visibility_clause = build_visibility_clause(es_index_name);
                    json! {
                        {
                            "bool": {
                                "must": [query],
                                "filter": [visibility_clause]
                            }
                        }
                    }
                };

                let mut left_field = link.left_field.clone().expect("no left field");
                if left_field.contains('.') {
                    let mut parts = left_field.splitn(2, '.');
                    parts.next();
                    left_field = parts.next().unwrap().to_string();
                }

                json! {
                    {
                        "subselect": {
                            "index": es_index_name,
                            "alias": index_options.alias(),
                            "type": "_doc",
                            "left_fieldname": left_field,
                            "right_fieldname": link.right_field,
                            "query": query
                        }
                    }
                }
            }
        }
    }
}

pub fn term_to_dsl(
    field: &QualifiedField,
    term: &Term,
    opcode: ComparisonOpcode,
) -> serde_json::Value {
    match opcode {
        ComparisonOpcode::Contains | ComparisonOpcode::Eq => eq(field, term, false),

        ComparisonOpcode::DoesNotContain | ComparisonOpcode::Ne => {
            json! { { "bool": { "must_not": [ eq(field, term, false) ] } } }
        }

        ComparisonOpcode::Regex => regex(field, term),

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
        // ComparisonOpcode::MoreLikeThis => {}
        // ComparisonOpcode::FuzzyLikeThis => {}
        _ => panic!("unsupported opcode {:?}", opcode),
    }
}

fn eq(field: &QualifiedField, term: &Term, is_span: bool) -> serde_json::Value {
    let clause = match term {
        Term::Null => {
            json! { { "bool": { "must_not": [ { "exists": { "field": field.field_name() } } ] } } }
        }
        Term::MatchAll => {
            if is_span {
                json! { { "wildcard": { field.field_name(): { "value": "*" } } } }
            } else {
                json! { { "exists": { "field": field.field_name() } } }
            }
        }
        Term::String(s, b) => {
            if s.contains('\\') {
                let s = unescape::unescape(s);
                if is_span {
                    return json! { { "span_term": { field.field_name(): { "value": s, "boost": b.unwrap_or(1.0) } } } };
                } else {
                    json! { { "match": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                }
            } else {
                if is_span {
                    return json! { { "span_term": { field.field_name(): { "value": s, "boost": b.unwrap_or(1.0) } } } };
                } else {
                    json! { { "match": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                }
            }
        }
        Term::Phrase(s, b) | Term::PhraseWithWildcard(s, b) => {
            if is_span {
                match ProximityTerm::make_proximity_chain(field, s, *b) {
                    ProximityTerm::ProximityChain(v) => proximity_chain(field, &v),
                    other => eq(field, &other.to_term(), true),
                }
            } else {
                if s.contains('\\') {
                    let s = unescape::unescape(s);
                    json! { { "match_phrase": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                } else {
                    json! { { "match_phrase": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                }
            }
        }
        Term::Prefix(s, b) => {
            json! { { "prefix": { field.field_name(): { "value": s[..s.len()-1], "boost": b.unwrap_or(1.0) } } } }
        }
        Term::PhrasePrefix(s, b) => {
            json! { { "match_phrase_prefix": { field.field_name(): { "query": s[..s.len()-1], "boost": b.unwrap_or(1.0) } } } }
        }
        Term::Wildcard(w, b) => {
            json! { { "wildcard": { field.field_name(): { "value": w, "boost": b.unwrap_or(1.0) } } } }
        }
        Term::Fuzzy(f, d, b) => {
            json! { { "fuzzy": { field.field_name(): { "value": f, "prefix_length": d, "boost": b.unwrap_or(1.0) } } } }
        }
        Term::Regex(r, b) => {
            json! { { "regexp": { field.field_name(): { "value": r, "boost": b.unwrap_or(1.0) } } } }
        }

        Term::Range(s, e, b) => {
            json! { { "range": { field.field_name(): { "gte": s, "lte": e, "boost": b.unwrap_or(1.0) }} } }
        }
        Term::ParsedArray(v, _b) => {
            let mut clauses = Vec::new();

            for t in v {
                match t {
                    Term::String(_, _)
                    | Term::Phrase(_, _)
                    | Term::Prefix(_, _)
                    | Term::PhrasePrefix(_, _)
                    | Term::Wildcard(_, _)
                    | Term::Regex(_, _)
                    | Term::Fuzzy(_, _, _) => clauses.push(eq(field, t, false)),
                    _ => panic!("unsupported term in an array: {:?}", t),
                }
            }

            if clauses.len() == 1 {
                clauses.pop().unwrap()
            } else {
                json! { { "bool": { "should": clauses } } }
            }
        }
        Term::UnparsedArray(s, _b) => {
            let tokens: Vec<&str> = s
                .split(|c: char| c.is_whitespace() || ",\"'[]".contains(c))
                .filter(|v| !v.is_empty())
                .collect();

            json! { { "terms": { field.field_name(): tokens } } }
        }
        Term::ProximityChain(parts) => proximity_chain(field, parts),
    };

    if is_span && !matches!(term,Term::ProximityChain { .. }) {
        json! { { "span_multi": { "match": clause } } }
    } else {
        clause
    }
}

fn proximity_chain(field: &QualifiedField, parts: &Vec<ProximityPart>) -> serde_json::Value {
    let mut clauses = Vec::new();

    for part in parts {
        if part.words.len() == 1 {
            clauses.push((
                eq(field, &part.words.get(0).unwrap().to_term(), true),
                &part.distance,
            ));
        } else {
            let mut spans = Vec::new();
            for word in &part.words {
                spans.push(eq(field, &word.to_term(), true));
            }

            clauses.push((
                json! {
                    { "span_or": { "clauses": spans } }
                },
                &part.distance,
            ));
        }
    }

    let mut span_near = None;
    let mut clauses = clauses.into_iter();
    while let Some((clause, distance)) = clauses.next() {
        let distance = distance.unwrap_or_default();
        let span = if let Some((next_clause, _)) = clauses.next() {
            json! {
                { "span_near": { "clauses": [ clause, next_clause ], "slop": distance.distance, "in_order": distance.in_order } }
            }
        } else {
            clause
        };

        span_near = Some(if span_near.is_none() {
            span
        } else {
            json! {
                { "span_near": { "clauses": [ span_near, span ], "slop": distance.distance, "in_order": distance.in_order } }
            }
        });
    }

    span_near.expect("did not generate a span_near clause")
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
            json! { { "regexp": { field.field_name(): { "value": r, "boost": b.unwrap_or(1.0) }}}}
        }
        _ => panic!("unsupported term for a regex query: {}", term),
    }
}
