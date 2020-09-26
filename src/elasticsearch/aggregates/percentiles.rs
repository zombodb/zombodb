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
    percents: Option<default!(Json, NULL)>,
) -> impl std::iter::Iterator<Item = (name!(key, f64), name!(value, Numeric))> {
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

    let elasticsearch = Elasticsearch::new(&index);

    let percentiles = Percentiles {
        field,
        percents,
        keyed: false,
    };

    let request = elasticsearch.aggregate::<PercentilesAggData>(
        query.prepare(),
        json! {
            {
                "percentiles": percentiles,
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result
        .values
        .into_iter()
        .map(|entry| (entry.key, entry.value))
}
