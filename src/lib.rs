use pgx::*;

mod access_method;
mod elasticsearch;
mod zdbquery;

pg_module_magic!();

#[pg_extern]
fn version() -> &'static str {
    "5.0"
}

mod tests {
    use pgx::*;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_version() {
        assert_eq!("5.0", crate::version());
    }
}
