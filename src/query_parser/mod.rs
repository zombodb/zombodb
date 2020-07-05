#![allow(unused_macros)]
use pgx::*;
mod ast;
mod parser;

#[allow(non_snake_case)]
macro_rules! Box {
    ($val:expr) => {
        Box::new($val)
    };
}

#[allow(non_snake_case)]
macro_rules! String {
    ($operator:tt, $field:expr, $val:expr, $boost:expr) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            $field,
            Box!(crate::query_parser::ast::Expr::String($val, Some($boost)))
        ))
    };
    ($operator:tt, $field:expr, $val:expr) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            $field,
            Box!(crate::query_parser::ast::Expr::String($val, None))
        ))
    };
}

#[allow(non_snake_case)]
macro_rules! Wildcard {
    ($operator:tt, $field:expr, $val:expr, $boost:expr) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            $field,
            Box!(crate::query_parser::ast::Expr::Wildcard($val, Some($boost)))
        ))
    };
    ($operator:tt, $field:expr, $val:expr) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            $field,
            Box!(crate::query_parser::ast::Expr::Wildcard($val, None))
        ))
    };
}

#[allow(non_snake_case)]
macro_rules! Fuzzy {
    ($operator:tt, $field:expr, $val:expr, $slop:expr, $boost:expr) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            $field,
            Box!(crate::query_parser::ast::Expr::Fuzzy(
                $val,
                $slop,
                Some($boost)
            ))
        ))
    };
    ($operator:tt, $field:expr, $val:expr, $slop:expr) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            $field,
            Box!(crate::query_parser::ast::Expr::Fuzzy($val, $slop, None))
        ))
    };
}

#[allow(non_snake_case)]
macro_rules! Not {
    ($e:expr) => {
        Box!(crate::query_parser::ast::Expr::Not($e))
    };
}

#[allow(non_snake_case)]
macro_rules! With {
    ($left:expr, $right:expr) => {
        Box!(crate::query_parser::ast::Expr::With($left, $right))
    };
}

#[allow(non_snake_case)]
macro_rules! And {
    ($left:expr, $right:expr) => {
        Box!(crate::query_parser::ast::Expr::And($left, $right))
    };
}

#[allow(non_snake_case)]
macro_rules! AndNot {
    ($left:expr, $right:expr) => {
        Box!(crate::query_parser::ast::Expr::And($left, Not!($right)))
    };
}

#[allow(non_snake_case)]
macro_rules! Or {
    ($left:expr, $right:expr) => {
        Box!(crate::query_parser::ast::Expr::Or($left, $right))
    };
}

#[pg_extern]
fn test_parser(input: &str) -> String {
    let expr = ast::Expr::from_str("_zdb_all", input).expect("failed to parse");
    format!("{:#}\n{:#?}", expr, expr)
}

#[cfg(test)]
mod string_tests {
    use crate::query_parser::ast::{Expr, ParserError};

    pub(super) fn parse(input: &str) -> Result<Box<Expr>, ParserError> {
        Expr::from_str("_", input)
    }

    fn assert_str(input: &str, expected: &str) {
        assert_eq!(
            &format!("{}", parse(input).expect("failed to parse")),
            expected
        )
    }

    #[test]
    fn string() {
        assert_str("foo", r#"_:"foo""#)
    }

    #[test]
    fn number() {
        assert_str("42", r#"_:"42""#)
    }

    #[test]
    fn float() {
        assert_str("42.42424242", r#"_:"42.42424242""#)
    }

    #[test]
    fn bool_true() {
        assert_str("true", r#"_:"true""#)
    }

    #[test]
    fn bool_false() {
        assert_str("false", r#"_:"false""#)
    }

    #[test]
    fn null() {
        assert_str("null", r#"_:NULL"#)
    }

    #[test]
    fn wildcard_star() {
        assert_str("foo*", r#"_:"foo*""#)
    }

    #[test]
    fn wildcard_question() {
        assert_str("foo?", r#"_:"foo?""#)
    }

    #[test]
    fn fuzzy() {
        assert_str("foo~2", r#"_:"foo"~2"#)
    }

    #[test]
    fn boost() {
        assert_str("foo^2.0", r#"_:"foo"^2"#)
    }

    #[test]
    fn boost_float() {
        assert_str("foo^.42", r#"_:"foo"^0.42"#)
    }

    #[test]
    fn single_quoted() {
        assert_str("'foo'", r#"_:"foo""#)
    }

    #[test]
    fn double_quoted() {
        assert_str(r#""foo""#, r#"_:"foo""#)
    }

    #[test]
    fn escape() {
        assert_str(r#"f\%o"#, r#"_:"f\%o""#)
    }

    #[test]
    fn parens() {
        assert_str(r#"(foo)"#, r#"_:"foo""#);
        assert_str(r#"((foo))"#, r#"_:"foo""#);
        assert_str(r#"(((foo)))"#, r#"_:"foo""#);
    }

    #[test]
    fn field() {
        assert_str("field:value", r#"field:"value""#)
    }

    #[test]
    fn field_group() {
        assert_str(
            "field:(a, b, c)",
            r#"((field:"a" OR field:"b") OR field:"c")"#,
        )
    }
}

#[cfg(test)]
mod expr_tests {
    use crate::query_parser::ast::Expr;
    use crate::query_parser::string_tests::parse;

    fn assert_expr<'input>(input: &'input str, expected: Box<Expr<'input>>) {
        assert_eq!(
            parse(input).expect("failed to parse"),
            expected,
            "{}",
            input
        );
    }

    #[test]
    fn wildcard_star() {
        assert_expr("foo*", Wildcard!(Contains, "_", "foo*"));
        assert_expr("*foo", Wildcard!(Contains, "_", "*foo"));
        assert_expr("*foo*", Wildcard!(Contains, "_", "*foo*"));
        assert_expr("f*o", Wildcard!(Contains, "_", "f*o"));
        assert_expr("*", Wildcard!(Contains, "_", "*"));
    }

    #[test]
    fn wildcard_question() {
        assert_expr("foo?", Wildcard!(Contains, "_", "foo?"));
        assert_expr("?foo", Wildcard!(Contains, "_", "?foo"));
        assert_expr("?foo?", Wildcard!(Contains, "_", "?foo?"));
        assert_expr("?", Wildcard!(Contains, "_", "?"));
    }

    #[test]
    fn fuzzy() {
        assert_expr("foo~2", Fuzzy!(Contains, "_", "foo", 2))
    }

    #[test]
    fn boost() {
        assert_expr("foo^2.0", String!(Contains, "_", "foo", 2.0))
    }

    #[test]
    fn not() {
        assert_expr("not foo", Not!(String!(Contains, "_", "foo")))
    }

    #[test]
    fn bang() {
        assert_expr("!foo", Not!(String!(Contains, "_", "foo")))
    }

    #[test]
    fn field() {
        assert_expr("field:value", String!(Contains, "field", "value"))
    }

    #[test]
    fn field_group() {
        assert_expr(
            "field:(a, b, c)",
            Or!(
                Or!(
                    String!(Contains, "field", "a"),
                    String!(Contains, "field", "b")
                ),
                String!(Contains, "field", "c")
            ),
        )
    }

    #[test]
    fn with() {
        assert_expr(
            "foo with bar",
            With!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn percent() {
        assert_expr(
            "foo%bar",
            With!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn and() {
        assert_expr(
            "foo and bar",
            And!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn ampersand() {
        assert_expr(
            "foo&bar",
            And!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn and_not() {
        assert_expr(
            "foo and not bar",
            AndNot!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn and_bang() {
        assert_expr(
            "foo&!bar",
            AndNot!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn or() {
        assert_expr(
            "foo or bar",
            Or!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn comma() {
        assert_expr(
            "foo,bar",
            Or!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[test]
    fn precedence() {
        assert_expr(
            "a or b with c and d and not not e",
            Or!(
                String!(Contains, "_", "a"),
                AndNot!(
                    And!(
                        With!(String!(Contains, "_", "b"), String!(Contains, "_", "c")),
                        String!(Contains, "_", "d")
                    ),
                    Not!(String!(Contains, "_", "e"))
                )
            ),
        )
    }

    #[test]
    fn operators() {
        assert_expr("a:b", String!(Contains, "a", "b"));
        assert_expr("a=b", String!(Eq, "a", "b"));
        assert_expr("a!=b", String!(Ne, "a", "b"));
        assert_expr("a<>b", String!(DoesNotContain, "a", "b"));
        assert_expr("a<b", String!(Lt, "a", "b"));
        assert_expr("a>b", String!(Gt, "a", "b"));
        assert_expr("a<=b", String!(Lte, "a", "b"));
        assert_expr("a>=b", String!(Gte, "a", "b"));
        assert_expr("a:~b", String!(Regex, "a", "b"));
        assert_expr("a:@b", String!(MoreLikeThis, "a", "b"));
        assert_expr("a:@~b", String!(FuzzyLikeThis, "a", "b"));
    }
}
