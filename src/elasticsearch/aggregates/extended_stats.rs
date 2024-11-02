use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgrx::prelude::*;
use pgrx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn extended_stats(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
    sigma: default!(i64, 0),
) -> TableIterator<(
    name!(count, i64),
    name!(min, AnyNumeric),
    name!(max, AnyNumeric),
    name!(avg, AnyNumeric),
    name!(sum, AnyNumeric),
    name!(sum_of_squares, AnyNumeric),
    name!(variance, AnyNumeric),
    name!(std_deviation, AnyNumeric),
    name!(upper, AnyNumeric),
    name!(lower, AnyNumeric),
)> {
    #[derive(Deserialize, Serialize)]
    struct ExtendedStatsAggData {
        count: i64,
        min: AnyNumeric,
        max: AnyNumeric,
        avg: AnyNumeric,
        sum: AnyNumeric,
        sum_of_squares: AnyNumeric,
        variance: AnyNumeric,
        std_deviation: AnyNumeric,
        std_deviation_bounds: StdDeviationBounds,
    }

    #[derive(Deserialize, Serialize)]
    struct StdDeviationBounds {
        upper: AnyNumeric,
        lower: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<ExtendedStatsAggData>(
        Some(field.into()),
        true,
        prepared_query,
        json! {
            {
                "extended_stats": {
                    "field" : field,
                    "sigma" : sigma
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(vec![(
        result.count,
        result.min,
        result.max,
        result.avg,
        result.sum,
        result.sum_of_squares,
        result.variance,
        result.std_deviation,
        result.std_deviation_bounds.upper,
        result.std_deviation_bounds.lower,
    )])
}
