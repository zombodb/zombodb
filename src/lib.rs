#![recursion_limit = "256"]
#![allow(clippy::type_complexity)]
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::redundant_closure)]
use pgx::*;

mod access_method;
mod cat;
mod elasticsearch;
mod executor_manager;
mod gucs;
mod highlighting;
mod json;
mod mapping;
mod misc;
pub mod query_dsl;
pub mod scoring;
mod utils;
mod walker;
mod zdbquery;
pub mod zql;

pg_module_magic!();

extension_sql_file!("../sql/_bootstrap.sql", bootstrap);
extension_sql_file!("../sql/_mappings.sql", name = "mappings");
extension_sql_file!(
    "../sql/_support-views.sql",
    name = "support_views",
    requires = [query_dsl::bool::dsl, "cat_api"],
);
extension_sql_file!("../sql/_join-support.sql", name = "join_support");
extension_sql_file!("../sql/_cat-api.sql", name = "cat_api");
extension_sql_file!(
    "../sql/_type-conversions.sql",
    name = "type_conversions",
    requires = [
        "mappings",
        "cat_api",
        query_dsl::geo::point_to_json,
        query_dsl::geo::point_array_to_json
    ]
);
extension_sql_file!("../sql/_finalize.sql", finalize);

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    zql::init();
    gucs::init();
    executor_manager::hooks::init_hooks();
    access_method::options::init();
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_fini() {
    // noop
}

#[pg_extern(immutable, parallel_safe)]
fn internal_version() -> String {
    #[allow(dead_code)]
    mod built_info {
        // The file has been placed there by the build script.
        include!(concat!(env!("OUT_DIR"), "/built.rs"));
    }

    format!(
        "{} ({}) --{}",
        built_info::PKG_VERSION,
        built_info::GIT_COMMIT_HASH.unwrap_or("no git commit hash available"),
        if built_info::DEBUG {
            "debug"
        } else {
            "release"
        }
    )
}

/// exists for debugging purposes
#[pg_extern(immutable, parallel_safe)]
fn ctid(as_u64: i64) -> pg_sys::ItemPointerData {
    let as_u64 = as_u64 as u64;
    let mut ctid = pg_sys::ItemPointerData::default();
    u64_to_item_pointer(as_u64, &mut ctid);
    ctid
}

#[cfg(test)]
mod testing;

#[cfg(test)]
pub mod pg_test {
    use crate::testing;

    pub fn setup(options: Vec<&str>) {
        testing::initialize_tests(options);
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "zdb.default_elasticsearch_url = 'http://localhost:19200/'",
            "enable_seqscan = false",
            "enable_indexscan = true",
        ]
    }
}
