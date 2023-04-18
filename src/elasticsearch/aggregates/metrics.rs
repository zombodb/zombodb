use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgrx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn sum(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct SumAggData {
        value: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<SumAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
fn avg(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct AvgAggData {
        value: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<AvgAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
fn cardinality(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct CardinalityAggData {
        value: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<CardinalityAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
fn max(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct MaxAggData {
        value: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<MaxAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
fn min(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct MinAggData {
        value: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<MinAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
fn missing(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct MissingAggData {
        doc_count: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<MissingAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
fn value_count(index: PgRelation, field: &str, query: ZDBQuery) -> AnyNumeric {
    #[derive(Deserialize, Serialize)]
    struct ValueCountAggData {
        value: AnyNumeric,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<ValueCountAggData>(
        Some(field.into()),
        true,
        prepared_query,
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
