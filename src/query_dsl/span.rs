//! This Mod is to...
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/span-queries.html
//!
//!Span queries are low-level positional queries which provide expert control over the order and proximity of the specified terms

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    fn span_containing(little: ZDBQuery, big: ZDBQuery) -> ZDBQuery {}

    fn span_first(query: ZDBQuery, end: i64) -> ZDBQuery {}

    fn span_masking(field: &str, query: ZDBQuery) -> ZDBQuery {}

    fn span_multi(query: ZDBQuery) -> ZDBQuery {}

    fn span_near(in_order: bool, slop: i64, clauses: AnyArry) -> ZDBQuery {}

    fn span_not(
        include: ZDBQuery,
        exclude: ZDBQuery,
        pre_integer: Default!(Option<i64>, NULL),
        post_integer: Default!(Option<i64>, NULL),
        dis_integer: Default!(Option<i64>, NULL),
    ) -> ZDBQuery {
    }

    fn span_or(clauses: AnyArry) -> ZDBQuery {}

    fn span_term(field: &str, value: &str, boost: Default!(Option<i64>, NULL)) -> ZDBQuery {}

    fn span_within(little: ZDBQuery, big: ZDBQuery) -> ZDBQuery {}
}

mod tests {}
