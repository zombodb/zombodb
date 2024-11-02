//! This module is to...
//! https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-sort.html
//!
//! Allows you to add one or more sorts on specific fields. Each sort can be reversed as well.

#[pgrx::pg_schema]
mod dsl {
    use crate::zdbquery::{
        SortDescriptor, SortDescriptorOptions, SortDirection, SortMode, ZDBQuery,
    };
    use pgrx::*;

    #[pg_extern(immutable, parallel_safe)]
    fn sd(
        field: &str,
        order: SortDirection,
        mode: default!(Option<SortMode>, NULL),
    ) -> SortDescriptor {
        SortDescriptor {
            field: field.to_string(),
            options: SortDescriptorOptions {
                order,
                mode,
                nested_path: None,
                nested_filter: None,
            },
        }
    }

    #[pg_extern(immutable, parallel_safe)]
    fn sd_nested(
        field: &str,
        order: SortDirection,
        nested_path: &str,
        nested_filter: default!(Option<ZDBQuery>, NULL),
        mode: default!(Option<SortMode>, NULL),
    ) -> SortDescriptor {
        let nested_filter = nested_filter.map(|query| query.query_dsl());
        SortDescriptor {
            field: field.to_string(),
            options: SortDescriptorOptions {
                order,
                mode,
                nested_path: Some(nested_path.to_string()),
                nested_filter,
            },
        }
    }

    #[pg_extern(immutable, parallel_safe)]
    fn sort(sort_field: &str, sort_direction: SortDirection, zdbquery: ZDBQuery) -> ZDBQuery {
        zdbquery.set_sort_descriptors(vec![Some(SortDescriptor {
            field: sort_field.to_string(),
            options: SortDescriptorOptions {
                order: sort_direction,
                mode: None,
                nested_path: None,
                nested_filter: None,
            },
        })])
    }

    #[pg_extern(immutable, parallel_safe)]
    fn sort_many(zdbquery: ZDBQuery, sort_descriptors: VariadicArray<SortDescriptor>) -> ZDBQuery {
        zdbquery.set_sort_descriptors(sort_descriptors)
    }

    #[pg_extern(immutable, parallel_safe)]
    fn sort_direct(sort_json: Json, zdbquery: ZDBQuery) -> ZDBQuery {
        zdbquery.set_sort_json(Some(sort_json.0))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgrx::*;
    use serde_json::*;

    #[pg_test]
    fn test_sort() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.sort('the_field', 'asc', 'david')")
            .expect("SPI failed")
            .expect("SPI datum was NULL");

        assert_eq!(
            &serde_json::to_value(&zdbquery).unwrap(),
            &json!(
                {
                    "query_dsl": { "query_string": {"query": "david"}},
                    "sort_json": [ {"the_field" : { "order": "asc"} }]
                }
            )
        );
    }

    #[pg_test]
    fn test_sort_direct() {
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.sort_direct('{\"foo\": \"bar\"}', 'david')")
                .expect("SPI failed")
                .expect("SPI datum was NULL");

        assert_eq!(
            &serde_json::to_value(&zdbquery).unwrap(),
            &json!(
                {
                    "query_dsl": { "query_string": {"query": "david"}},
                    "sort_json": { "foo": "bar"}
                }
            )
        );
    }

    #[pg_test]
    fn test_sort_sd_and_sort_many() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.sort_many('query',
                dsl.sd('cat','asc','max'),
                dsl.sd('dog','asc','min'),
                dsl.sd('foo','desc','sum')
             )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");

        assert_eq!(
            &serde_json::to_value(&zdbquery).unwrap(),
            &json!(
                {
                    "query_dsl": { "query_string": {"query": "query"}},
                    "sort_json": [
                                   {"cat": { "mode": "max", "order": "asc"} },
                                   {"dog": { "mode": "min", "order": "asc"} },
                                   {"foo": { "mode": "sum", "order": "desc"} }
                                 ]
                }
            )
        );
    }

    #[pg_test]
    fn test_sort_sd_nested_and_sort_many() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.sort_many('query',
                dsl.sd_nested('cat', 'asc', 'a_path', dsl.term('fieldname','filter'), 'max'),
                dsl.sd_nested('dog', 'asc', 'a_path', dsl.term('fieldname','filter'), 'min'),
                dsl.sd_nested('foo', 'desc', 'a_path', dsl.term('fieldname','filter'), 'sum')
             )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");

        assert_eq!(
            &serde_json::to_value(&zdbquery).unwrap(),
            &json! {
                {
                  "query_dsl": {
                    "query_string": {
                      "query": "query"
                    }
                  },
                  "sort_json": [
                    {
                      "cat": {
                        "mode": "max",
                        "nested_filter": {
                          "term": {
                            "fieldname": {
                              "value": "filter",
                            }
                          }
                        },
                        "nested_path": "a_path",
                        "order": "asc"
                      }
                    },
                    {
                      "dog": {
                        "mode": "min",
                        "nested_filter": {
                          "term": {
                            "fieldname": {
                              "value": "filter",
                            }
                          }
                        },
                        "nested_path": "a_path",
                        "order": "asc"
                      }
                    },
                    {
                      "foo": {
                        "mode": "sum",
                        "nested_filter": {
                          "term": {
                            "fieldname": {
                              "value": "filter",
                            }
                          }
                        },
                        "nested_path": "a_path",
                        "order": "desc"
                      }
                    }
                  ]
                }
            }
        );
    }
}
