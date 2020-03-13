use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn matrix_stats(
    index: PgRelation,
    fields: Array<&str>,
    query: ZDBQuery,
) -> impl std::iter::Iterator<
    Item = (
        name!(term, String),
        name!(count, i64),
        name!(mean, f64),
        name!(variance, f64),
        name!(skewness, f64),
        name!(kurtosis, f64),
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
        mean: f64,
        variance: f64,
        skewness: f64,
        kurtosis: f64,
        covariance: serde_json::Value,
        correlation: serde_json::Value,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<MatrixStatsAggData>(
        query,
        json! {
            {
                "matrix_stats": {
                    "fields": fields,
                }
            }
        },
    );

    info!("got here before request was executed");

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.fields.unwrap_or_default().into_iter().map(|entry| {
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
    })
}
