mod dsl {
    use crate::query_parser::ast::IndexLink;
    use crate::query_parser::dsl::expr_to_dsl;
    use crate::query_parser::INDEX_LINK_PARSER;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use std::collections::HashSet;

    #[pg_extern(immutable, parallel_safe)]
    fn zdb(index: PgRelation, input: &str) -> ZDBQuery {
        let mut used_fields = HashSet::new();
        let index_links = IndexLink::from_zdb(&index);
        let query = crate::query_parser::ast::Expr::from_str(
            &index,
            "zdb_all",
            input,
            &index_links,
            &None,
            &mut used_fields,
        )
        .expect("failed to parse query");
        ZDBQuery::new_with_query_dsl(expr_to_dsl(
            &IndexLink::from_relation(&index),
            &index_links,
            &query,
        ))
    }

    #[pg_extern(immutable, parallel_stafe)]
    fn link_options(options: Vec<Option<String>>, query: ZDBQuery) -> ZDBQuery {
        query.set_link_options(
            options
                .into_iter()
                .filter_map(|e| {
                    if e.is_some() {
                        INDEX_LINK_PARSER.with(|parser| {
                            let mut used_fields = HashSet::new();
                            let mut fieldname_stack = Vec::new();
                            let mut operator_stack = Vec::new();

                            Some(
                                parser
                                    .parse(
                                        None,
                                        &mut used_fields,
                                        &mut fieldname_stack,
                                        &mut operator_stack,
                                        &e.unwrap(),
                                    )
                                    .expect("failed to parse index link"),
                            )
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }
}
