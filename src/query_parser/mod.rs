#![allow(unused_macros)]
use crate::query_parser::ast::QualifiedIndex;
use pgx::*;
use std::collections::HashSet;

pub mod ast;
mod parser;

#[pg_extern]
fn test_parser(input: &str) -> String {
    let mut used_fields = HashSet::new();
    let expr = ast::Expr::from_str(
        QualifiedIndex {
            schema: None,
            table: "table".to_string(),
            index: "index".to_string(),
        },
        "_zdb_all",
        input,
        &mut used_fields,
    )
    .expect("failed to parse");
    format!("{}\n{:#?}", expr, expr)
}

#[cfg(test)]
#[macro_use]
mod macros {
    #[allow(non_snake_case)]
    macro_rules! Box {
        ($val:expr) => {
            Box::new($val)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! String {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::String($val.into(), Some($boost))
            ))
        };
        ($operator:tt, $table:literal, $index:literal, $field:literal, $val:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: $table.to_string(),
                        index: $index.to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::String($val.into(), None)
            ))
        };
        ($operator:tt, $field:literal, $val:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::String($val.into(), None)
            ))
        };
        ($val:expr) => {
            crate::query_parser::ast::Term::String($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Wildcard {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::Wildcard($val.into(), Some($boost))
            ))
        };
        ($operator:tt, $field:literal, $val:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::Wildcard($val.into(), None)
            ))
        };
        ($val:expr) => {
            crate::query_parser::ast::Term::Wildcard($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Fuzzy {
        ($operator:tt, $field:literal, $val:expr, $slop:expr, $boost:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::Fuzzy($val, $slop, Some($boost))
            ))
        };
        ($operator:tt, $field:literal, $val:expr, $slop:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::Fuzzy($val, $slop, None)
            ))
        };
        ($val:expr, $slop:expr) => {
            crate::query_parser::ast::Term::Fuzzy($val, $slop, None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! UnparsedArray {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::UnparsedArray($val, Some($boost))
            ))
        };
        ($operator:tt, $field:literal, $val:expr) => {
            Box!(crate::query_parser::ast::Expr::$operator(
                crate::query_parser::ast::QualifiedField {
                    index: crate::query_parser::ast::QualifiedIndex {
                        schema: None,
                        table: "table".to_string(),
                        index: "index".to_string()
                    },
                    field: $field.to_string()
                },
                crate::query_parser::ast::Term::UnparsedArray($val, None)
            ))
        };
        ($val:expr) => {
            crate::query_parser::ast::Term::UnparsedArray($val, None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! ParsedArray {
    ($operator:tt, $field:literal, $($elements:expr),*) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            crate::query_parser::ast::QualifiedField{
                index: crate::query_parser::ast::QualifiedIndex {
                    schema: None,
                    table: "table".to_string(),
                    index: "index".to_string()
                },
                field: $field.to_string()
            },
            crate::query_parser::ast::Term::ParsedArray(
                vec![$($elements),*],
                None
            )
        ))
    };
}

    #[allow(non_snake_case)]
    macro_rules! ParsedArrayWithBoost {
    ($operator:tt, $field:literal, $boost:expr, $($elements:expr),*) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            crate::query_parser::ast::QualifiedField{
                index: crate::query_parser::ast::QualifiedIndex {
                    schema: None,
                    table: "table".to_string(),
                    index: "index".to_string()
                },
                field: $field.to_string()
            },
            crate::query_parser::ast::Term::ParsedArray(
                vec![$($elements),*],
                Some($boost)
            )
        ))
    };
}

    #[allow(non_snake_case)]
    macro_rules! ProximityChain {
    ($operator:tt, $field:literal, $($parts:expr),*) => {
        Box!(crate::query_parser::ast::Expr::$operator(
            crate::query_parser::ast::QualifiedField{
                index: crate::query_parser::ast::QualifiedIndex {
                    schema: None,
                    table: "table".to_string(),
                    index: "index".to_string()
                },
                field: $field.to_string()
            },
            crate::query_parser::ast::Term::ProximityChain(vec![$($parts),*])
        ))
    };
}

    #[allow(non_snake_case)]
    macro_rules! Within {
        ($left:expr, $distance:literal, $in_order:literal) => {
            crate::query_parser::ast::ProximityPart {
                words: $left,
                distance: Some(crate::query_parser::ast::ProximityDistance {
                    distance: $distance,
                    in_order: $in_order,
                }),
            }
        };
        ($left:expr) => {
            crate::query_parser::ast::ProximityPart {
                words: $left,
                distance: None,
            }
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
    macro_rules! Or {
        ($left:expr, $right:expr) => {
            Box!(crate::query_parser::ast::Expr::Or($left, $right))
        };
    }
}

#[cfg(test)]
mod string_tests {
    use crate::query_parser::ast::{Expr, ParserError, QualifiedIndex};
    use std::collections::HashSet;

    pub(super) fn parse(input: &str) -> Result<Box<Expr>, ParserError> {
        let mut used_fields = HashSet::new();
        Expr::from_str(
            QualifiedIndex {
                schema: None,
                table: "table".to_string(),
                index: "index".to_string(),
            },
            "_",
            input,
            &mut used_fields,
        )
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
    fn regex() {
        assert_str("field:~'regex goes here'", r#"field:~"regex goes here""#)
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

    #[test]
    fn parsed_array() {
        assert_str("[a,b,c]", r#"_:["a","b","c"]"#)
    }
    #[test]
    fn unparsed_array() {
        assert_str("[[1,2,3]]", "_:[[1,2,3]]")
    }
}

#[cfg(test)]
mod expr_tests {
    use crate::query_parser::ast::{Expr, IndexLink, QualifiedIndex};
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
            And!(
                String!(Contains, "_", "foo"),
                Not!(String!(Contains, "_", "bar"))
            ),
        )
    }

    #[test]
    fn and_bang() {
        assert_expr(
            "foo&!bar",
            And!(
                String!(Contains, "_", "foo"),
                Not!(String!(Contains, "_", "bar"))
            ),
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
            "a or b with c and d and not not (e or f)",
            Or!(
                String!(Contains, "_", "a"),
                And!(
                    And!(
                        With!(String!(Contains, "_", "b"), String!(Contains, "_", "c")),
                        String!(Contains, "_", "d")
                    ),
                    Not!(Not!(Or!(
                        String!(Contains, "_", "e"),
                        String!(Contains, "_", "f")
                    )))
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

    #[test]
    fn prox() {
        assert_expr(
            "a w/2 b",
            ProximityChain!(
                Contains,
                "_",
                Within!(vec![String!("a")], 2, false),
                Within!(vec![String!("b")])
            ),
        )
    }

    #[test]
    fn prox_groups() {
        assert_expr(
            "(a, b, c) wo/2 (x, y, z)",
            ProximityChain!(
                Contains,
                "_",
                Within!(vec![String!("a"), String!("b"), String!("c")], 2, true),
                Within!(vec![String!("x"), String!("y"), String!("z")])
            ),
        )
    }

    #[test]
    fn prox_mixed_groups() {
        assert_expr(
            "(a, b, c) w/8 foo wo/2 (x, y, z)",
            ProximityChain!(
                Contains,
                "_",
                Within!(vec![String!("a"), String!("b"), String!("c")], 8, false),
                Within!(vec![String!("foo")], 2, true),
                Within!(vec![String!("x"), String!("y"), String!("z")])
            ),
        )
    }

    #[test]
    fn prox_groups_field() {
        assert_expr(
            "field:((a, b, c) wo/2 (x, y, z))",
            ProximityChain!(
                Contains,
                "field",
                Within!(vec![String!("a"), String!("b"), String!("c")], 2, true),
                Within!(vec![String!("x"), String!("y"), String!("z")])
            ),
        )
    }

    #[test]
    fn paresd_array() {
        assert_expr(
            "[a,b,c]",
            ParsedArray!(Contains, "_", String!("a"), String!("b"), String!("c")),
        )
    }

    #[test]
    fn paresd_with_wildcard() {
        assert_expr(
            "[a,b,c*]",
            ParsedArray!(Contains, "_", String!("a"), String!("b"), Wildcard!("c*")),
        )
    }

    #[test]
    fn paresd_array_with_boost() {
        assert_expr(
            "[a,b,c]^42",
            ParsedArrayWithBoost!(
                Contains,
                "_",
                42.0,
                String!("a"),
                String!("b"),
                String!("c")
            ),
        )
    }

    #[test]
    fn unparsed_array() {
        assert_expr("[[a, b,   c]]", UnparsedArray!(Contains, "_", "a, b,   c"))
    }

    #[test]
    fn json() {
        assert_expr(
            r#"({"field":    "value"})"#,
            Box::new(Expr::Json(r#"{"field":"value"}"#.to_string())),
        )
    }

    #[test]
    fn subselect() {
        assert_expr(
            "#subselect<id=<other.index>o_id>(value), outer",
            Or!(
                Box::new(Expr::Subselect(
                    IndexLink {
                        name: None,
                        left_field: "id",
                        qualified_index: QualifiedIndex {
                            schema: None,
                            table: "other".to_string(),
                            index: "index".to_string()
                        },
                        right_field: "o_id",
                    },
                    String!(Contains, "other", "index", "_", "value"),
                )),
                String!(Contains, "_", "outer")
            ),
        )
    }
}
