mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub fn fuzzy(
        field: &str,
        value: &str,
        boost: Option<default!(f32, null)>,
        fuzziness_integer: Option<default!(i64, null)>,
        prefix_length: Option<default!(i64, null)>,
        max_expansions: Option<default!(i64, 50)>,
        transpositions: Option<default!(bool, null)>,
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
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

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
