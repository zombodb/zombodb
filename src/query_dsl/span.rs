//! This Mod is to...
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/span-queries.html
//!
//!Span queries are low-level positional queries which provide expert control over the order and proximity of the specified terms

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    fn span_containing(little: ZDBQuery, big: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_containing" : {
                    "little" :  little.query_dsl(),
                    "big" : big.query_dsl()
                }
            }
        })
    }

    fn span_first(query: ZDBQuery, end: i64) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_first" :query.query_dsl(),
                "end" : end
            }
        })
    }

    fn span_masking(field: &str, query: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
              {
          "field_masking_span" :
                  query.query_dsl(),
                  "field": field
              }
        })
    }

    fn span_multi(query: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_multi": query.query_dsl()
            }
        })
    }

    fn span_near(in_order: bool, slop: i64, clauses: Array<ZDBQuery>) -> ZDBQuery {
        let mut vec = Vec::new();
        for element in clauses.iter() {
            match element {
                Some(zdbquery) => {
                    let query_dsl = zdbquery.query_dsl();
                    match query_dsl {
                        Some(query_dsl) => vec.push(query_dsl.clone()),
                        None => {}
                    }
                }
                None => {}
            }
        }
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_near" : {
                    "clauses" : clauses,
                    "slop" : slop,
                    "in_order" : in_order,
                }
            }
        })
    }

    fn span_not(
        include: ZDBQuery,
        exclude: ZDBQuery,
        pre_integer: default!(Option<i64>, NULL),
        post_integer: default!(Option<i64>, NULL),
        dis_integer: default!(Option<i64>, NULL),
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_not" : {
                    "include" : include.query_dsl(),
                    "exclude" : exclude.query_dsl(),
                    "pre" : pre_integer,
                    "post" : post_integer,
                    "dis" : dis_integer,
                }
            }
        })
    }

    fn span_or(clauses: Array<ZDBQuery>) -> ZDBQuery {
        let mut vec = Vec::new();
        for element in clauses.iter() {
            match element {
                Some(zdbquery) => {
                    let query_dsl = zdbquery.query_dsl();
                    match query_dsl {
                        Some(query_dsl) => vec.push(query_dsl.clone()),
                        None => {}
                    }
                }
                None => {}
            }
        }
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_or" : {
                    "clauses" :  vec
                }
            }
        })
    }

    fn span_term(field: &str, value: &str, boost: default!(Option<i64>, NULL)) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_term" :{
                    field : {
                        "term"  : value,
                        "boost" : boost
                    }
                }
            }
        })
    }

    fn span_within(little: ZDBQuery, big: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_containing" : {
                    "little" :  little.query_dsl(),
                    "big" : big.query_dsl()
                }
            }
        })
    }
}
