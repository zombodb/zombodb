use pgx::*;

mod access_method;
mod custom_scan;
mod elasticsearch;
mod json;
mod query_dsl;
mod zdbquery;

pg_module_magic!();

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    access_method::options::init();
    custom_scan::init();
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_fini() {}

#[pg_extern]
fn version() -> &'static str {
    "5.0"
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_version() {
        assert_eq!("5.0", crate::version());
    }
}
