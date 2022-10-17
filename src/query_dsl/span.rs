//! This Mod is to...
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/span-queries.html
//!
//!Span queries are low-level positional queries which provide expert control over the order and proximity of the specified terms

#[pgx_macros::pg_schema]
mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_containing(little: ZDBQuery, big: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_containing" : {
                    "little" :  little.into_value(),
                    "big" : big.into_value()
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_first(query: ZDBQuery, end: i64) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_first" :query.into_value(),
                "end" : end
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_masking(field: &str, query: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
              {
          "field_masking_span" :
                  query.into_value(),
                  "field": field
              }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_multi(query: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_multi": query.into_value()
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_near(
        in_order: bool,
        slop: i64,
        clauses: VariadicArray<ZDBQuery>,
    ) -> ZDBQuery {
        let clauses: Vec<serde_json::Value> = clauses
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .into_value()
            })
            .collect();

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

    #[derive(Serialize)]
    struct SpanNot<'a> {
        include: &'a Value,
        exclude: &'a Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        pre_integer: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        post_integer: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        dis_integer: Option<i64>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_not(
        include: ZDBQuery,
        exclude: ZDBQuery,
        pre_integer: default!(Option<i64>, NULL),
        post_integer: default!(Option<i64>, NULL),
        dis_integer: default!(Option<i64>, NULL),
    ) -> ZDBQuery {
        let include = include.into_value();
        let exclude = exclude.into_value();
        let span_not = SpanNot {
            include: &include,
            exclude: &exclude,
            pre_integer,
            post_integer,
            dis_integer,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_not" :
                            span_not
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn span_or(clauses: VariadicArray<ZDBQuery>) -> ZDBQuery {
        let clauses: Vec<serde_json::Value> = clauses
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .into_value()
            })
            .collect();
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_or" : {
                    "clauses" :  clauses
                }
            }
        })
    }

    #[derive(Serialize)]
    struct SpanTerm<'a> {
        value: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_term(
        field: &str,
        value: &str,
        boost: default!(Option<f32>, NULL),
    ) -> ZDBQuery {
        let span_terms = SpanTerm { value, boost };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_term" :{
                    field :
                        span_terms
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn span_within(little: ZDBQuery, big: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "span_within" : {
                    "little" :  little.into_value(),
                    "big" : big.into_value()
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use crate::query_dsl::span::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_span_term_without_boost() {
        let zdbquery = span_term("term_field", "term_value", None);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_term" :{
                        "term_field" : {
                            "value"  : "term_value"
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_span_term_with_boost() {
        let term_boost = 2.9 as f32;
        let zdbquery = span_term("term_field", "term_value", Some(term_boost));
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_term" :{
                        "term_field" : {
                            "value"  : "term_value",
                            "boost" : term_boost
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_span_containing() {
        let little_boost = 2.9 as f32;
        let zdbquery = span_containing(
            span_term("little_field", "little_value", Some(little_boost)),
            span_term("big_field", "big_value", None),
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                   "span_containing" : {
                        "little" :{
                            "span_term" :{
                                "little_field" : {
                                    "value"  : "little_value",
                                    "boost" : little_boost
                                }
                            }
                        },
                        "big" :{
                            "span_term" :{
                                 "big_field" : {
                                    "value"  : "big_value",
                                 }
                            }
                        }
                    }
                }
            }
        );
    }

    #[pg_test()]
    fn test_span_first() {
        let zdbquery = span_first(span_term("span_term_field", "span_term_value", None), 50);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_first" :{
                           "span_term" :{
                                     "span_term_field" : {
                                        "value"  : "span_term_value",
                                     }
                                }
                     },
                    "end" : 50
                }
            }
        );
    }

    #[pg_test()]
    fn test_span_masking() {
        let zdbquery = span_masking(
            "span_masking_field",
            span_term("span_term_field", "span_term_value", None),
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "field_masking_span" :{
                           "span_term" :{
                                     "span_term_field" : {
                                        "value"  : "span_term_value",
                                     }
                                }
                     },
                    "field" : "span_masking_field"
                }
            }
        );
    }

    #[pg_test()]
    fn test_span_multi() {
        let zdbquery = span_multi(span_term("span_term_field", "span_term_value", None));
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_multi" :{
                           "span_term" :{
                                "span_term_field" : {
                                    "value"  : "span_term_value",
                                }
                            }
                     },
                }
            }
        );
    }

    #[pg_test(error = "found NULL zdbquery in clauses")]
    fn test_span_near_with_null() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.span_near(
                true, 
                50, 
                dsl.term('field1', 'value1'), 
                dsl.term('field2', 'value2'), 
                null /* this causes the error */
            )",
        )
        .expect("failed to get SPI result");
        zdbquery.limit(); //using this to prevent an warning that zdbquery is not used
    }

    #[pg_test]
    fn test_span_near_with_null_inorder() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.span_near(
                null, 
                50, 
                dsl.term('field1', 'value1'), 
                dsl.term('field2', 'value2')
            )",
        );

        assert!(zdbquery.is_none());
    }

    #[pg_test]
    fn test_span_near_without_nulls() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.span_near(
                true, 
                50, 
                dsl.span_term('span_term_field1', 'span_term_value1'), 
                dsl.span_term('span_term_field2', 'span_term_value2')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_near" : {
                        "clauses" : [
                        {
                            "span_term" :{
                                "span_term_field1" : {
                                    "value"  : "span_term_value1",
                                }
                            }
                            },
                            {
                            "span_term" :{
                                "span_term_field2" : {
                                    "value"  : "span_term_value2",
                                }
                            }
                            }
                        ],
                    "slop" : 50,
                    "in_order" : true,
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_span_within() {
        let little_boost = 2.9 as f32;
        let zdbquery = span_within(
            span_term("little_field", "little_value", Some(little_boost)),
            span_term("big_field", "big_value", None),
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                   "span_within" : {
                        "little" :{
                            "span_term" :{
                                "little_field" : {
                                    "value"  : "little_value",
                                    "boost" : little_boost
                                }
                            }
                        },
                        "big" :{
                            "span_term" :{
                                 "big_field" : {
                                    "value"  : "big_value",
                                 }
                            }
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_span_or() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.span_or(
                    dsl.span_term('span_term_field1', 'span_term_value1'), 
                    dsl.span_term('span_term_field2', 'span_term_value2'),
                    dsl.span_term('span_term_field3', 'span_term_value3')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_or" : {
                        "clauses" : [
                        {
                            "span_term" :{
                                "span_term_field1" : {
                                    "value"  : "span_term_value1",
                                }
                            }
                            },
                            {
                            "span_term" :{
                                "span_term_field2" : {
                                    "value"  : "span_term_value2",
                                }
                            }
                            },
                            {
                            "span_term" :{
                                "span_term_field3" : {
                                    "value"  : "span_term_value3",
                                }
                            }
                            }
                        ],
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_span_not_without_default() {
        let zdbquery = span_not(
            span_term("included_field", "included_value", None),
            span_term("excluded_field", "excluded_value", None),
            None,
            None,
            None,
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_not" : {
                        "include" : {
                                "span_term" : {
                                    "included_field" :{
                                        "value" : "included_value"
                                    }
                                }
                        },
                        "exclude" : {
                                "span_term" : {
                                    "excluded_field" : {
                                        "value": "excluded_value"
                                     }
                                }
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_span_not_with_default() {
        let term_boost = 2.9 as f32;
        let zdbquery = span_not(
            span_term("included_field", "included_value", Some(term_boost)),
            span_term("excluded_field", "excluded_value", None),
            None,
            None,
            None,
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "span_not" : {
                        "include" : {
                                "span_term" : {
                                    "included_field" :{
                                        "value" : "included_value",
                                        "boost" : term_boost
                                    }
                                }
                        },
                        "exclude" : {
                                "span_term" : {
                                    "excluded_field" : {
                                        "value": "excluded_value"
                                     }
                                }
                        }
                    }
                }
            }
        );
    }
}
