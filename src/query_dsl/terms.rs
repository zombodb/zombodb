use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde_json::*;
use std::iter::FromIterator;

#[pg_extern]
fn terms(field: &str, values: Array<&str>, boost: Option<f32>) -> ZDBQuery {
    let values = Vec::from_iter(values.iter());

    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: values,
                "boost": boost.unwrap_or(1.0)
            }
        }
    })
}
