#![allow(unused_macros)]

use std::collections::{HashMap, HashSet};

use crate::zql::ast::QualifiedField;

pub mod ast;
pub mod dsl;
pub mod parser;

pub(crate) mod relationship_manager;
pub(crate) mod transformations;

// global parsers to avoid recurring regex compilation
thread_local! {
    pub static ZDB_QUERY_PARSER: crate::zql::parser::ExprParser = crate::zql::parser::ExprParser::new();
    pub static FIELD_LIST_PARSER: crate::zql::parser::FieldListParser = crate::zql::parser::FieldListParser::new();
    pub static INDEX_LINK_PARSER: crate::zql::parser::IndexLinkParser = crate::zql::parser::IndexLinkParser::new();
}

pub(crate) fn init() {
    ZDB_QUERY_PARSER.with(|_parser| ());
    FIELD_LIST_PARSER.with(|_parser| ());
    INDEX_LINK_PARSER.with(|_parser| ());
}

pub(crate) fn parse_field_lists(input: &str) -> HashMap<String, Vec<QualifiedField>> {
    let mut used_fields = HashSet::new();
    let mut fieldname_stack = Vec::new();
    let mut operator_stack = Vec::new();
    let field_list = FIELD_LIST_PARSER.with(|parser| {
        parser
            .parse(
                None,
                &mut used_fields,
                &mut fieldname_stack,
                &mut operator_stack,
                input,
            )
            .expect("failed to parse field lists")
    });

    let mut qualified_field_list = HashMap::new();
    qualified_field_list.extend(field_list.into_iter().map(|(k, v)| {
        (
            k,
            v.into_iter()
                .map(|field| QualifiedField { index: None, field })
                .collect(),
        )
    }));

    qualified_field_list
}

#[cfg(any(test, feature = "pg_test"))]
#[macro_use]
mod macros {
    #[allow(non_snake_case)]
    macro_rules! Box {
        ($val:expr) => {
            Box::new($val)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! MatchAll {
        ($operator:tt, $field:literal) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::MatchAll,
            )
        };
    }

    #[allow(non_snake_case)]
    macro_rules! String {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::String($val.into(), Some($boost)),
            )
        };
        ($operator:tt, $table:literal, $index:literal, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::String($val.into(), None),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::String($val.into(), None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::Term::String($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! ProximityString {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::ProximityTerm::String($val.into(), None),
            )
        };
        ($operator:tt, $table:literal, $index:literal, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::ProximityTerm::String($val.into(), None),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::ProximityTerm::String($val.into(), None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::ProximityTerm::String($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Wildcard {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Wildcard($val.into(), Some($boost)),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Wildcard($val.into(), None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::Term::Wildcard($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Prefix {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Prefix($val.into(), Some($boost)),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Prefix($val.into(), None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::Term::Prefix($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! PhrasePrefix {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::PhrasePrefix($val.into(), Some($boost)),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::PhrasePrefix($val.into(), None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::Term::PhrasePrefix($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Regex {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Regex($val.into(), Some($boost)),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Regex($val.into(), None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::Term::Regex($val.into(), None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Fuzzy {
        ($operator:tt, $field:literal, $val:expr, $slop:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Fuzzy($val, $slop, Some($boost)),
            )
        };
        ($operator:tt, $field:literal, $val:expr, $slop:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::Fuzzy($val, $slop, None),
            )
        };
        ($val:expr, $slop:expr) => {
            crate::zql::ast::Term::Fuzzy($val, $slop, None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! UnparsedArray {
        ($operator:tt, $field:literal, $val:expr, $boost:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::UnparsedArray($val, Some($boost)),
            )
        };
        ($operator:tt, $field:literal, $val:expr) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField {
                    index: Some(IndexLink::default()),
                    field: $field.into(),
                },
                crate::zql::ast::Term::UnparsedArray($val, None),
            )
        };
        ($val:expr) => {
            crate::zql::ast::Term::UnparsedArray($val, None)
        };
    }

    #[allow(non_snake_case)]
    macro_rules! ParsedArray {
        ($operator:tt, $field:literal, $($elements:expr),*) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField{
                    index: Some(IndexLink::default()),
                    field: $field.into()
                },
                crate::zql::ast::Term::ParsedArray(
                    vec![$($elements),*],
                    None
                )
            )
        };
    }

    #[allow(non_snake_case)]
    macro_rules! ParsedArrayWithBoost {
        ($operator:tt, $field:literal, $boost:expr, $($elements:expr),*) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField{
                    index: Some(IndexLink::default()),
                    field: $field.into()
                },
                crate::zql::ast::Term::ParsedArray(
                    vec![$($elements),*],
                    Some($boost)
                )
            )
        };
    }

    #[allow(non_snake_case)]
    macro_rules! ProximityChain {
        ($operator:tt, $field:literal, $($parts:expr),*) => {
            crate::zql::ast::Expr::$operator(
                crate::zql::ast::QualifiedField{
                    index: Some(IndexLink::default()),
                    field: $field.into()
                },
                crate::zql::ast::Term::ProximityChain(vec![$($parts),*])
            )
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Within {
        ($left:expr, $distance:literal, $in_order:literal) => {
            crate::zql::ast::ProximityPart {
                words: $left,
                distance: Some(crate::zql::ast::ProximityDistance {
                    distance: $distance,
                    in_order: $in_order,
                }),
            }
        };
        ($left:expr) => {
            crate::zql::ast::ProximityPart {
                words: $left,
                distance: None,
            }
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Not {
        ($e:expr) => {
            crate::zql::ast::Expr::Not(Box!($e))
        };
    }

    #[allow(non_snake_case)]
    macro_rules! With {
        ($left:expr, $right:expr) => {
            crate::zql::ast::Expr::WithList(vec![$left, $right])
        };
    }

    #[allow(non_snake_case)]
    macro_rules! And {
        ($left:expr, $right:expr) => {
            crate::zql::ast::Expr::AndList(vec![$left, $right])
        };
        ($one:expr, $two:expr, $three:expr) => {
            crate::zql::ast::Expr::AndList(vec![$one, $two, $three])
        };
    }

    #[allow(non_snake_case)]
    macro_rules! Or {
        ($left:expr, $right:expr) => {
            crate::zql::ast::Expr::OrList(vec![$left, $right])
        };
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use std::collections::{HashMap, HashSet};

    use pgx::*;

    use crate::zql::ast::{Expr, IndexLink, ParserError, QualifiedIndex};

    pub(super) fn parse(input: &str) -> Result<Expr, ParserError> {
        let mut used_fields = HashSet::new();
        Expr::from_str_disconnected(
            None,
            "_",
            input,
            &mut used_fields,
            IndexLink {
                name: None,
                left_field: None,
                qualified_index: QualifiedIndex {
                    schema: None,
                    table: "table".to_string(),
                    index: "index".to_string(),
                },
                right_field: None,
            },
            &Vec::new(),
            &None,
            HashMap::new(),
        )
    }

    fn assert_str(input: &str, expected: &str) {
        assert_eq!(
            &format!("{}", parse(input).expect("failed to parse")),
            expected
        )
    }

    #[pg_test]
    fn test_string_string() {
        assert_str("foo", r#"_:"foo""#)
    }

    #[pg_test]
    fn test_string_number() {
        assert_str("42", r#"_:"42""#)
    }

    #[pg_test]
    fn test_string_float() {
        assert_str("42.42424242", r#"_:"42.42424242""#)
    }

    #[pg_test]
    fn test_string_bool_true() {
        assert_str("true", r#"_:"true""#)
    }

    #[pg_test]
    fn test_string_bool_false() {
        assert_str("false", r#"_:"false""#)
    }

    #[pg_test]
    fn test_string_null() {
        assert_str("null", r#"_:NULL"#)
    }

    #[pg_test]
    fn test_string_wildcard_star() {
        assert_str("foo*", r#"_:"foo*""#)
    }

    #[pg_test]
    fn test_string_wildcard_question() {
        assert_str("foo?", r#"_:"foo?""#)
    }

    #[pg_test]
    fn test_string_fuzzy() {
        assert_str("foo~2", r#"_:"foo"~2"#)
    }

    #[pg_test]
    fn test_string_regex() {
        assert_str("field:~'^m.*$'", r#"field:~"^m.*$""#)
    }

    #[pg_test]
    fn test_string_boost() {
        assert_str("foo^2.0", r#"_:"foo"^2"#)
    }

    #[pg_test]
    fn test_string_boost_float() {
        assert_str("foo^.42", r#"_:"foo"^0.42"#)
    }

    #[pg_test]
    fn test_string_single_quoted() {
        assert_str("'foo'", r#"_:"foo""#)
    }

    #[pg_test]
    fn test_string_double_quoted() {
        assert_str(r#""foo""#, r#"_:"foo""#)
    }

    #[pg_test]
    fn test_string_escape() {
        assert_str(r#"f\%o"#, r#"_:"f\%o""#)
    }

    #[pg_test]
    fn test_string_parens() {
        assert_str(r#"(foo)"#, r#"_:"foo""#);
        assert_str(r#"((foo))"#, r#"_:"foo""#);
        assert_str(r#"(((foo)))"#, r#"_:"foo""#);
    }

    #[pg_test]
    fn test_string_field() {
        assert_str("field:value", r#"field:"value""#)
    }

    #[pg_test]
    fn test_string_field_group() {
        assert_str(
            "field:(a, b, c)",
            r#"(field:"a" OR field:"b" OR field:"c")"#,
        )
    }

    #[pg_test]
    fn test_string_parsed_array() {
        assert_str("[a,b,c]", r#"_:["a","b","c"]"#)
    }
    #[pg_test]
    fn test_string_unparsed_array() {
        assert_str("[[1,2,3]]", "_:[[1,2,3]]")
    }

    fn assert_expr<'input>(input: &'input str, expected: Expr<'input>) {
        assert_eq!(
            parse(input).expect("failed to parse"),
            expected,
            "{}",
            input
        );
    }

    #[pg_test]
    fn test_non_ascii_string() {
        assert_expr("é", String!(Contains, "_", "é"))
    }

    #[pg_test]
    fn test_expr_regex() {
        assert_expr("field:~'^m.*$'", Regex!(Regex, "field", "^m.*$"))
    }

    #[pg_test]
    fn test_expr_wildcard_star() {
        assert_expr("'foo bar*'", PhrasePrefix!(Contains, "_", "foo bar*"));
        assert_expr("foo*", Prefix!(Contains, "_", "foo*"));
        assert_expr("*foo", Wildcard!(Contains, "_", "*foo"));
        assert_expr("*foo*", Wildcard!(Contains, "_", "*foo*"));
        assert_expr("f*o", Wildcard!(Contains, "_", "f*o"));
        assert_expr("*", MatchAll!(Contains, "_"));
    }

    #[pg_test]
    fn test_expr_wildcard_question() {
        assert_expr("foo?", Wildcard!(Contains, "_", "foo?"));
        assert_expr("?foo", Wildcard!(Contains, "_", "?foo"));
        assert_expr("?foo?", Wildcard!(Contains, "_", "?foo?"));
        assert_expr("?", Wildcard!(Contains, "_", "?"));
    }

    #[pg_test]
    fn test_expr_fuzzy() {
        assert_expr("foo~2", Fuzzy!(Contains, "_", "foo", 2))
    }

    #[pg_test]
    fn test_expr_fuzzy_default() {
        assert_expr("foo~", Fuzzy!(Contains, "_", "foo", 3))
    }

    #[pg_test]
    fn test_expr_boost() {
        assert_expr("foo^2.0", String!(Contains, "_", "foo", 2.0))
    }

    #[pg_test]
    fn test_expr_not() {
        assert_expr("not foo", Not!(String!(Contains, "_", "foo")))
    }

    #[pg_test]
    fn test_expr_bang() {
        assert_expr("!foo", Not!(String!(Contains, "_", "foo")))
    }

    #[pg_test]
    fn test_expr_field() {
        assert_expr("field:value", String!(Contains, "field", "value"))
    }

    #[pg_test]
    fn test_expr_field_group() {
        assert_expr(
            "field:(a, b, c)",
            Expr::OrList(vec![
                String!(Contains, "field", "a"),
                String!(Contains, "field", "b"),
                String!(Contains, "field", "c"),
            ]),
        )
    }

    #[pg_test]
    fn test_expr_with() {
        assert_expr(
            "a.foo:foo with a.bar:bar",
            With!(
                String!(Contains, "a.bar", "bar"),
                String!(Contains, "a.foo", "foo")
            ),
        )
    }

    #[pg_test]
    fn test_expr_percent() {
        assert_expr(
            "a.foo:foo%a.bar:bar",
            With!(
                String!(Contains, "a.bar", "bar"),
                String!(Contains, "a.foo", "foo")
            ),
        )
    }

    #[pg_test]
    fn test_expr_and() {
        assert_expr(
            "foo and bar",
            And!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[pg_test]
    fn test_expr_and_is_default_operator() {
        assert_expr(
            "foo bar",
            And!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[pg_test]
    fn test_expr_ampersand() {
        assert_expr(
            "foo&bar",
            And!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[pg_test]
    fn test_expr_and_not() {
        assert_expr(
            "foo and not bar",
            And!(
                String!(Contains, "_", "foo"),
                Not!(String!(Contains, "_", "bar"))
            ),
        )
    }

    #[pg_test]
    fn test_expr_and_bang() {
        assert_expr(
            "foo&!bar",
            And!(
                String!(Contains, "_", "foo"),
                Not!(String!(Contains, "_", "bar"))
            ),
        )
    }

    #[pg_test]
    fn test_expr_or() {
        assert_expr(
            "foo or bar",
            Or!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[pg_test]
    fn test_expr_comma() {
        assert_expr(
            "foo,bar",
            Or!(String!(Contains, "_", "foo"), String!(Contains, "_", "bar")),
        )
    }

    #[pg_test]
    fn test_expr_precedence() {
        assert_expr(
            "a or (a.b:b with a.c:c) and (d and not (not (e or f)))",
            Or!(
                String!(Contains, "_", "a"),
                And!(
                    And!(
                        With!(String!(Contains, "a.c", "c"), String!(Contains, "a.b", "b")),
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

    // same result as [test_expr_precedence], just different formatting
    #[pg_test]
    fn test_expr_precedence_no_groupings() {
        assert_expr(
            "a or a.b:b with a.c:c and d and not not (e or f)",
            Or!(
                String!(Contains, "_", "a"),
                And!(
                    And!(
                        With!(String!(Contains, "a.c", "c"), String!(Contains, "a.b", "b")),
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

    #[pg_test]
    fn test_expr_operators() {
        assert_expr("a:b", String!(Contains, "a", "b"));
        assert_expr("a=b", String!(Eq, "a", "b"));
        assert_expr("a!=b", String!(Ne, "a", "b"));
        assert_expr("a<>b", String!(DoesNotContain, "a", "b"));
        assert_expr("a<b", String!(Lt, "a", "b"));
        assert_expr("a>b", String!(Gt, "a", "b"));
        assert_expr("a<=b", String!(Lte, "a", "b"));
        assert_expr("a>=b", String!(Gte, "a", "b"));
        assert_expr("a:~b", Regex!(Regex, "a", "b"));
        assert_expr("a:~'.*'", Regex!(Regex, "a", ".*"));
        assert_expr("a:@b", String!(MoreLikeThis, "a", "b"));
        assert_expr("a:@~b", String!(FuzzyLikeThis, "a", "b"));
        assert_expr("a==>b", String!(Matches, "a", "b"));
    }

    #[pg_test]
    fn test_expr_prox() {
        assert_expr(
            "a w/2 b",
            ProximityChain!(
                Contains,
                "_",
                Within!(vec![ProximityString!("a")], 2, false),
                Within!(vec![ProximityString!("b")])
            ),
        )
    }

    #[pg_test]
    fn test_expr_prox_groups() {
        assert_expr(
            "(a, b, c) wo/2 (x, y, z)",
            ProximityChain!(
                Contains,
                "_",
                Within!(
                    vec![
                        ProximityString!("a"),
                        ProximityString!("b"),
                        ProximityString!("c")
                    ],
                    2,
                    true
                ),
                Within!(vec![
                    ProximityString!("x"),
                    ProximityString!("y"),
                    ProximityString!("z")
                ])
            ),
        )
    }

    #[pg_test]
    fn test_expr_prox_mixed_groups() {
        assert_expr(
            "(a, b, c) w/8 foo wo/2 (x, y, z)",
            ProximityChain!(
                Contains,
                "_",
                Within!(
                    vec![
                        ProximityString!("a"),
                        ProximityString!("b"),
                        ProximityString!("c")
                    ],
                    8,
                    false
                ),
                Within!(vec![ProximityString!("foo")], 2, true),
                Within!(vec![
                    ProximityString!("x"),
                    ProximityString!("y"),
                    ProximityString!("z")
                ])
            ),
        )
    }

    #[pg_test]
    fn test_expr_prox_groups_field() {
        assert_expr(
            "field:((a, b, c) wo/2 (x, y, z))",
            ProximityChain!(
                Contains,
                "field",
                Within!(
                    vec![
                        ProximityString!("a"),
                        ProximityString!("b"),
                        ProximityString!("c")
                    ],
                    2,
                    true
                ),
                Within!(vec![
                    ProximityString!("x"),
                    ProximityString!("y"),
                    ProximityString!("z")
                ])
            ),
        )
    }

    #[pg_test]
    fn test_expr_paresd_array() {
        assert_expr(
            "[a,b,c]",
            ParsedArray!(Contains, "_", String!("a"), String!("b"), String!("c")),
        )
    }

    #[pg_test]
    fn test_expr_paresd_with_wildcard() {
        assert_expr(
            "[a,b,c*]",
            ParsedArray!(Contains, "_", String!("a"), String!("b"), Prefix!("c*")),
        )
    }

    #[pg_test]
    fn test_expr_paresd_array_with_boost() {
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

    #[pg_test]
    fn test_expr_unparsed_array() {
        assert_expr("[[a, b,   c]]", UnparsedArray!(Contains, "_", "a, b,   c"))
    }

    #[pg_test]
    fn test_expr_json() {
        assert_expr(
            r#"({"field":    "value"})"#,
            Expr::Json(r#"{"field":"value"}"#.to_string()),
        )
    }

    #[pg_test]
    fn test_quoted_fieldname() {
        assert_expr(
            "`some:weird:field%`: value",
            String!(Contains, "some:weird:field%", "value"),
        )
    }

    #[pg_test]
    fn issue713() {
        assert_expr(
            "`some:weird:field%`: value AND `some:other:weird:field%`: value",
            And!(
                String!(Contains, "some:weird:field%", "value"),
                String!(Contains, "some:other:weird:field%", "value")
            ),
        )
    }
}
