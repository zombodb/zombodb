use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

#[pg_extern(immutable, parallel_safe)]
fn adjacency_matrix(
    index: PgRelation,
    labels: Array<&str>,
    filters: Array<ZDBQuery>,
) -> impl std::iter::Iterator<Item = (name!(term, Option<String>), name!(doc_count, i64))> {
    let elasticsearch = Elasticsearch::new(&index);

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
        key: serde_json::Value,
    }

    #[derive(Deserialize, Serialize)]
    struct AdjacencyMatrixAggData {
        buckets: Vec<BucketEntry>,
    }

    let mut filters_map = HashMap::new();
    for (label, filter) in labels.iter().zip(filters.iter()) {
        let label = label.expect("NULL labels are not allowed");
        let filter = filter.expect("NULL filters are not allowed");

        filters_map.insert(
            label,
            apply_visibility_clause(&elasticsearch, &filter, false),
        );
    }

    let request = elasticsearch.raw_json_aggregate::<AdjacencyMatrixAggData>(json! {
        {
            "adjacency_matrix": {
                "filters": filters_map,
            }
        }
    });

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result
        .buckets
        .into_iter()
        .map(|entry| (json_to_string(entry.key), entry.doc_count))
}

extension_sql!(
    r#"  

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_2x2(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2])

$$;

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_3x3(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2]),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term = labels[3])

$$;

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_4x4(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text, "4" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3], labels[4]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[4], labels[4]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2]),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[4], labels[4]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term = labels[3]),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[4], labels[4]||'&'||labels[3]))
   UNION ALL
SELECT labels[4],
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[1], labels[1]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[2], labels[2]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[3], labels[3]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term = labels[4])

$$;

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_5x5(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text, "4" text, "5" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3], labels[4], labels[5]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[4], labels[4]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[5], labels[5]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2]),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[4], labels[4]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[5], labels[5]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term = labels[3]),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[4], labels[4]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[5], labels[5]||'&'||labels[3]))
   UNION ALL
SELECT labels[4],
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[1], labels[1]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[2], labels[2]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[3], labels[3]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term = labels[4]),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[5], labels[5]||'&'||labels[4]))
   UNION ALL
SELECT labels[5],
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[1], labels[1]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[2], labels[2]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[3], labels[3]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[4], labels[4]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term = labels[5])

$$;


"#
);
