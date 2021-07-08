mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct Fuzzy<'a> {
        value: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzziness: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix_length: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_expansions: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        transpositions: Option<bool>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn fuzzy(
        field: &str,
        value: &str,
        boost: Option<default!(f32, "NULL")>,
        fuzziness: Option<default!(i32, "NULL")>,
        prefix_length: Option<default!(i64, "NULL")>,
        max_expansions: Option<default!(i64, 50)>,
        transpositions: Option<default!(bool, "NULL")>,
    ) -> ZDBQuery {
        let fuzzy_object = Fuzzy {
            value,
            boost,
            fuzziness,
            prefix_length,
            max_expansions,
            transpositions,
        };

        ZDBQuery::new_with_query_dsl(json! {
            {
                "fuzzy": {
                    field: fuzzy_object
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::query_dsl::fuzzy::dsl::*;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_fuzzy() {
        let zdbquery = fuzzy(
            "field",
            "value",
            Some(1.0),
            Some(10),
            Some(50),
            Some(50),
            Some(true),
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "fuzzy": {
                  "field": {
                        "value": "value",
                        "boost": 1.0,
                        "fuzziness": 10,
                        "max_expansions": 50,
                        "prefix_length": 50,
                        "transpositions": true,
                        }
                   }
                }
            }
        );
    }

    #[pg_test]
    fn test_fuzzy_with_default() {
        let zdbquery = fuzzy("field", "value", None, None, None, None, None);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "fuzzy": {
                  "field": {
                        "value": "value",
                        }
                   }
                }
            }
        );
    }
}
