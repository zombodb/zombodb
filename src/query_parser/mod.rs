use pgx::*;
mod ast;
mod parser;

#[pg_extern]
fn test_parser(input: &str) -> String {
    let parser = parser::ExprParser::new();
    let expr = parser.parse(input).expect("failed to parse");
    format!("{:#}\n{:#?}", expr, expr)
}
