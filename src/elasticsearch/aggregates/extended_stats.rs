use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn extended_stats(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
    sigma: default!(i64, 0),
) -> impl std::iter::Iterator<
    Item = (
        name!(count, i64),
        name!(min, Numeric),
        name!(max, Numeric),
        name!(avg, Numeric),
        name!(sum, Numeric),
        name!(sum_of_squares, Numeric),
        name!(variance, Numeric),
        name!(std_deviation, Numeric),
        name!(upper, Numeric),
        name!(lower, Numeric),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct ExtendedStatsAggData {
        count: i64,
        min: Numeric,
        max: Numeric,
        avg: Numeric,
        sum: Numeric,
        sum_of_squares: Numeric,
        variance: Numeric,
        std_deviation: Numeric,
        std_deviation_bounds: StdDeviationBounds,
    }

    #[derive(Deserialize, Serialize)]
    struct StdDeviationBounds {
        upper: Numeric,
        lower: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<ExtendedStatsAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
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

    vec![(
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
    )]
    .into_iter()
}
