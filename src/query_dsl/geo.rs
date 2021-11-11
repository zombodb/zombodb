use pgx::*;
use serde_json::json;

#[pgx_macros::pg_schema]
mod pg_catalog {
    use pgx::*;
    use serde::Serialize;

    #[derive(PostgresEnum, Serialize)]
    pub enum GeoShapeRelation {
        INTERSECTS,
        DISJOINT,
        WITHIN,
        CONTAINS,
    }

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum GeoBoundingBoxType {
        indexed,
        memory,
    }
}

#[pg_extern(immutable, parallel_safe)]
fn point_to_json(point: pg_sys::Point) -> Json {
    Json(json! {[point.x, point.y]})
}

#[pg_extern(immutable, parallel_safe)]
fn point_array_to_json(points: Array<pg_sys::Point>) -> Json {
    Json(
        json! { points.into_iter().map(|v| point_to_json(v.expect("null points are not allowed"))).collect::<Vec<Json>>() },
    )
}

#[pgx_macros::pg_schema]
mod dsl {
    use crate::query_dsl::geo::pg_catalog::{GeoBoundingBoxType, GeoShapeRelation};
    use crate::query_dsl::geo::point_array_to_json;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[pg_extern(immutable, parallel_safe)]
    fn geo_shape(field: &str, geojson_shape: Json, relation: GeoShapeRelation) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "geo_shape": {
                    field: {
                        "shape": geojson_shape,
                        "relation": relation
                    }
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn geo_bounding_box(
        field: &str,
        bounding_box: pg_sys::BOX,
        box_type: default!(GeoBoundingBoxType, "'memory'"),
    ) -> ZDBQuery {
        let high = bounding_box.high;
        let low = bounding_box.low;

        ZDBQuery::new_with_query_dsl(json! {
            {
                "geo_bounding_box": {
                    "type": box_type,
                    field: {
                        "left": high.x,
                        "top": high.y,
                        "right": low.x,
                        "bottom": low.y
                    }
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn geo_polygon(field: &str, points: variadic!(Array<pg_sys::Point>)) -> ZDBQuery {
        let points_json = point_array_to_json(points);
        ZDBQuery::new_with_query_dsl(json! {
            {
                "geo_polygon": {
                    field: {
                        "points": points_json
                    }
                }
            }
        })
    }
}

extension_sql_file!(
    "../../sql/_postgis-support.sql",
    name = "postgis_support",
    requires = ["mappings"]
);
