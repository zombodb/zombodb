//! This module is to...
//!https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
//!
//!Terms lookup fetches the field values of an existing document.
//! Elasticsearch then uses those values as search terms. This can be helpful when searching for a large set of terms.

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    fn termslookup(
        field: &str,
        index: &str,
        id: &str,
        path: &str,
        routing: Option<&str>,
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
        {
            "terms": {
                field : {
                    "index" : index,
                     "id" : id,
                     "path" : path,
                     "routing" : routing
                    }
                }
            }
        })
    }
}

mod test {
    use crate::query_dsl::termslookup::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use pgx_tests::*;
    use serde_json::json;
}
