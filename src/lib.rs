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
pub mod query_parser;
pub mod scoring;
mod utils;
mod walker;
mod zdbquery;

pg_module_magic!();

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    query_parser::init();
    gucs::init();
    executor_manager::hooks::init_hooks();
    access_method::options::init();
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_fini() {
    // noop
}

#[pg_extern]
fn version() -> &'static str {
    "5.0"
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_version() {
        assert_eq!("5.0", crate::version());
    }
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
