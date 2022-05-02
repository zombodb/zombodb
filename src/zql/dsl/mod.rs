use std::collections::HashSet;

use pgx::*;
use serde_json::json;

use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::aggregates::terms::terms_array_agg;
use crate::gucs::{ZDB_ACCELERATOR, ZDB_IGNORE_VISIBILITY};
use crate::utils::lookup_es_field_type;
use crate::zdbquery::mvcc::build_visibility_clause;
use crate::zdbquery::ZDBQuery;
use crate::zql::ast::{
    ComparisonOpcode, Expr, IndexLink, ProximityPart, ProximityTerm, QualifiedField, Term,
};

#[pg_extern(immutable, parallel_safe)]
fn dump_query(index: PgRelation, query: ZDBQuery) -> String {
    serde_json::to_string_pretty(query.prepare(&index, None).0.query_dsl())
        .expect("failed to convert DSL to text")
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
        &None,
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

        Expr::Subselect(link, e) => expr_to_dsl(link, index_links, e),
        Expr::Expand(link, e, _) => {
            expr_to_dsl(link, index_links, &Expr::Linked(link.clone(), e.clone()))
        }

        // AND and WITH output the same query DSL, but we want to maintain their differences in the AST
        Expr::AndList(v) | Expr::WithList(v) => {
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

        Expr::Matches(f, t) => term_to_dsl(f, t, ComparisonOpcode::Matches),

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
            let mut query_dsl = expr_to_dsl(root, index_links, e.as_ref());
            if link.left_field.is_none() {
                query_dsl
            } else {
                let target_relation = link.open_index().unwrap_or_else(|e| {
                    panic!("failed to open index '{}': {}", link.qualified_index, e)
                });
                let index_options = ZDBIndexOptions::from_relation(&target_relation);
                let es_index_name = index_options.index_name();

                let mut left_field = link.left_field.clone().expect("no left field");
                if left_field.contains('.') {
                    let mut parts = left_field.splitn(2, '.');
                    parts.next();
                    left_field = parts.next().unwrap().to_string();
                }

                if ZDB_ACCELERATOR.get() {
                    // the search accelerator is said to be installed, so lets use it to solve the join

                    // and we only need to apply the visibility filter if we're using the accelerator
                    query_dsl = if ZDB_IGNORE_VISIBILITY.get() {
                        query_dsl
                    } else {
                        let visibility_clause = build_visibility_clause(es_index_name);
                        json! {
                            {
                                "bool": {
                                    "must": [query_dsl],
                                    "filter": [visibility_clause]
                                }
                            }
                        }
                    };

                    json! {
                        {
                            "subselect": {
                                "index": es_index_name,
                                "alias": index_options.alias(),
                                "type": "_doc",
                                "left_fieldname": left_field,
                                "right_fieldname": link.right_field,
                                "query": query_dsl
                            }
                        }
                    }
                } else {
                    // otherwise, we'll do a poor-man's version where we just collect up the results
                    // of a `zdb.terms()` query
                    let terms_query = ZDBQuery::new_with_query_dsl(query_dsl);
                    let values = terms_array_agg(
                        target_relation,
                        link.right_field.as_ref().unwrap(),
                        terms_query,
                        Some(i32::MAX),
                        None,
                    );

                    json! {
                        {
                            "terms": {
                                left_field: values
                            }
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
        ComparisonOpcode::Matches => match term {
            Term::String(s, b)
            | Term::Phrase(s, b)
            | Term::PhrasePrefix(s, b)
            | Term::PhraseWithWildcard(s, b) => {
                if s.contains('\\') {
                    let s = unescape(s);
                    json! { { "match": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                } else {
                    json! { { "match": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                }
            }
            Term::Fuzzy(s, f, b) => {
                if s.contains('\\') {
                    let s = unescape(s);
                    json! { { "match": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0), "minimum_should_match": f } } } }
                } else {
                    json! { { "match": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0), "minimum_should_match": f } } } }
                }
            }
            _ => panic!("unsupported Term {:?} for Matches", term),
        },

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
                let s = unescape(s);
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
        Term::PhraseWithWildcard(s, b) => match ProximityTerm::make_proximity_chain(field, s, *b) {
            ProximityTerm::ProximityChain(v) => proximity_chain(field, &v),
            other => eq(field, &other.to_term(), true),
        },

        Term::Phrase(s, b) => {
            if is_span {
                match ProximityTerm::make_proximity_chain(field, s, *b) {
                    ProximityTerm::ProximityChain(v) => proximity_chain(field, &v),
                    other => eq(field, &other.to_term(), true),
                }
            } else {
                if s.contains('\\') {
                    let s = unescape(s);
                    json! { { "match_phrase": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                } else {
                    json! { { "match_phrase": { field.field_name(): { "query": s, "boost": b.unwrap_or(1.0) } } } }
                }
            }
        }
        Term::Prefix(s, b) => {
            if s.contains('\\') {
                let s = unescape(s);

                json! { { "prefix": { field.field_name(): { "value": s[..s.len()-1], "case_insensitive": true, "rewrite": "constant_score", "boost": b.unwrap_or(1.0) } } } }
            } else {
                json! { { "prefix": { field.field_name(): { "value": s[..s.len()-1], "case_insensitive": true, "rewrite": "constant_score", "boost": b.unwrap_or(1.0) } } } }
            }
        }
        Term::PhrasePrefix(s, b) => {
            if lookup_es_field_type(
                &field.index.as_ref().unwrap().open_index().unwrap(),
                &field.field_name(),
            ) == "keyword"
            {
                if s.contains('\\') {
                    let s = unescape(s);

                    json! { { "prefix": { field.field_name(): { "value": s[..s.len()-1], "case_insensitive": true, "rewrite": "constant_score", "boost": b.unwrap_or(1.0) } } } }
                } else {
                    json! { { "prefix": { field.field_name(): { "value": s[..s.len()-1], "case_insensitive": true, "rewrite": "constant_score", "boost": b.unwrap_or(1.0) } } } }
                }
            } else {
                if s.contains('\\') {
                    let s = unescape(s);
                    json! { { "match_phrase_prefix": { field.field_name(): { "query": s[..s.len()-1], "boost": b.unwrap_or(1.0) } } } }
                } else {
                    json! { { "match_phrase_prefix": { field.field_name(): { "query": s[..s.len()-1], "boost": b.unwrap_or(1.0) } } } }
                }
            }
        }
        Term::Wildcard(w, b) => {
            json! { { "wildcard": { field.field_name(): { "value": w, "boost": b.unwrap_or(1.0), "case_insensitive": true } } } }
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

    if is_span && !matches!(term, Term::ProximityChain { .. }) {
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
        Term::String(s, b) | Term::Phrase(s, b) => (s, b),
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

fn unescape(input: &str) -> String {
    let mut s = String::with_capacity(input.len());
    let mut prev_c = '\0';
    for c in input.chars() {
        if (c == '\\' && prev_c == '\\') || c != '\\' {
            s.push(c);
        }

        prev_c = c;
    }
    s
}
