use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgrx::prelude::*;
use pgrx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn stats(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
) -> TableIterator<(
    name!(count, i64),
    name!(min, AnyNumeric),
    name!(max, AnyNumeric),
    name!(avg, AnyNumeric),
    name!(sum, AnyNumeric),
)> {
    #[derive(Deserialize, Serialize)]
    struct StatsAggData {
        count: i64,
        min: AnyNumeric,
        max: AnyNumeric,
        avg: AnyNumeric,
        sum: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<StatsAggData>(
        Some(field.into()),
        true,
        prepared_query,
        json! {
            {
                "stats": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(
        vec![(result.count, result.min, result.max, result.avg, result.sum)].into_iter(),
    )
}
