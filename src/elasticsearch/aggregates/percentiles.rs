use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn percentiles(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
    percents: default!(Option<Json>, NULL),
) -> TableIterator<(name!(key, f64), name!(value, Numeric))> {
    #[derive(Deserialize, Serialize)]
    struct Entry {
        key: f64,
        value: Numeric,
    }

    #[derive(Deserialize, Serialize)]
    struct PercentilesAggData {
        values: Vec<Entry>,
    }

    #[derive(Serialize)]
    struct Percentiles<'a> {
        field: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        percents: Option<Json>,
        keyed: bool,
    }

    let percentiles = Percentiles {
        field,
        percents,
        keyed: false,
    };

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<PercentilesAggData>(
        Some(field.into()),
        true,
        prepared_query,
        json! {
            {
                "percentiles": percentiles,
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(result
        .values
        .into_iter()
        .map(|entry| (entry.key, entry.value)))
}
