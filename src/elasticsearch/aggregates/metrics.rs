use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn sum(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct SumAggData {
        value: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<SumAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "sum": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.value
}

#[pg_extern(immutable, parallel_safe)]
fn avg(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct AvgAggData {
        value: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<AvgAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "avg": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.value
}

#[pg_extern(immutable, parallel_safe)]
fn cardinality(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct CardinalityAggData {
        value: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<CardinalityAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "cardinality": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.value
}

#[pg_extern(immutable, parallel_safe)]
fn max(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct MaxAggData {
        value: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<MaxAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "max": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.value
}

#[pg_extern(immutable, parallel_safe)]
fn min(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct MinAggData {
        value: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<MinAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "min": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.value
}

#[pg_extern(immutable, parallel_safe)]
fn missing(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct MissingAggData {
        doc_count: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<MissingAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "missing": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.doc_count
}

#[pg_extern(immutable, parallel_safe)]
fn value_count(index: PgRelation, field: &str, query: ZDBQuery) -> Numeric {
    #[derive(Deserialize, Serialize)]
    struct ValueCountAggData {
        value: Numeric,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<ValueCountAggData>(
        Some(field.into()),
        true,
        query.prepare(&index),
        json! {
            {
                "value_count": {
                    "field" : field
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.value
}
