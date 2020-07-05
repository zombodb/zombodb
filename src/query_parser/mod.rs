use pgx::*;
mod ast;
mod parser;

#[pg_extern]
fn test_parser(input: &str) -> String {
    let expr = ast::Expr::from_str("_zdb_all", input).expect("failed to parse");
    format!("{:#}\n{:#?}", expr, expr)
}
