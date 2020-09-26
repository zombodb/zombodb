use crate::query_parser::ast::IndexLink;
use crate::query_parser::dsl::expr_to_dsl;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use std::collections::HashSet;

#[pg_extern(immutable, parallel_safe)]
pub fn zdb(index: PgRelation, input: &str) -> ZDBQuery {
    let mut used_fields = HashSet::new();
    let query =
        crate::query_parser::ast::Expr::from_str(&index, "zdb_all", input, &mut used_fields)
            .expect("failed to parse query");
    ZDBQuery::new_with_query_dsl(expr_to_dsl(&IndexLink::from_relation(&index), &query))
}
