use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::prelude::*;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn matrix_stats(
    index: PgRelation,
    fields: Array<&str>,
    query: ZDBQuery,
) -> TableIterator<
    'static,
    (
        name!(term, String),
        name!(count, i64),
        name!(mean, Numeric),
        name!(variance, Numeric),
        name!(skewness, Numeric),
        name!(kurtosis, Numeric),
        name!(covariance, Option<Json>),
        name!(correlation, Option<Json>),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct MatrixStatsAggData {
        doc_count: i64,
        fields: Option<Vec<Fields>>,
    }

    #[derive(Deserialize, Serialize)]
    struct Fields {
        name: String,
        count: i64,
        mean: Numeric,
        variance: Numeric,
        skewness: Numeric,
        kurtosis: Numeric,
        covariance: serde_json::Value,
        correlation: serde_json::Value,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<MatrixStatsAggData>(
        None,
        false,
        query.prepare(&index, None).0,
        json! {
            {
                "matrix_stats": {
                    "fields": fields,
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(result.fields.unwrap_or_default().into_iter().map(|entry| {
        (
            entry.name,
            entry.count,
            entry.mean,
            entry.variance,
            entry.skewness,
            entry.kurtosis,
            Some(Json(entry.covariance)),
            Some(Json(entry.correlation)),
        )
    }))
}
