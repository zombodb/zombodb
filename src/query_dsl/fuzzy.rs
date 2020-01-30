mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    pub fn fuzzy(
        field: &str,
        value: &str,
        boost: default!(f32, null),
        fuzziness_integer: default!(i64, null),
        prefix_length: default!(i64, null),
        max_expansions: default!(i64, 50),
        transpositions: default!(bool, null),
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
            "fuzzy": {
                  field: {
                        "value": value ,
                        "fuzziness": fuzziness_integer,
                        "max_expansions": max_expansions,
                        "prefix_length": prefix_length,
                        "transpositions": transpositions,
                        "rewrite": "constant_score"
                        }
                   }
            }
        })
    }
}

mod tests {
    use crate::query_dsl::fuzzy::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_fuzzy() {
        let zdbquery = fuzzy("field", "value", 1.0, 10, 50, 50, true);
        let dls = zdbquery.query_dsl();

        assert!(dls.is_some());
        assert_eq!(
            dls.unwrap(),
            &json! {
                {
                    "fuzzy": {
                  "field": {
                        "value": "value" ,
                        "fuzziness": 10,
                        "max_expansions": 50,
                        "prefix_length": 50,
                        "transpositions": true,
                        "rewrite": "constant_score"
                        }
                   }
                }
            }
        );
    }
}
