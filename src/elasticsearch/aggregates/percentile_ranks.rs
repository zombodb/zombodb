use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::prelude::*;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn percentile_ranks(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
    values: Json,
) -> TableIterator<(name!(key, f64), name!(value, AnyNumeric))> {
    #[derive(Deserialize, Serialize)]
    struct Entry {
        key: f64,
        value: AnyNumeric,
    }

    #[derive(Deserialize, Serialize)]
    struct PercentileRankAggData {
        values: Vec<Entry>,
    }

    #[derive(Serialize)]
    struct PercentileRanks<'a> {
        field: &'a str,
        values: Json,
        keyed: bool,
    }

    let percentile_ranks = PercentileRanks {
        field,
        values,
        keyed: false,
    };

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<PercentileRankAggData>(
        Some(field.into()),
        true,
        prepared_query,
        json! {
            {
                "percentile_ranks": percentile_ranks,
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(
        result
            .values
            .into_iter()
            .map(|entry| (entry.key, entry.value)),
    )
}
