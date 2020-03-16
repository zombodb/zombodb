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
        query,
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
        query,
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
        query,
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
        query,
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
        query,
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
        query,
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
        query,
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
