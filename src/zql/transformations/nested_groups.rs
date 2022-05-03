use std::collections::HashMap;

use pgx::PgRelation;

use crate::utils::{is_named_index_link, is_nested_field};
use crate::zql::ast::Expr;

pub fn group_nested(index: &Option<&PgRelation>, expr: &mut Expr) {
    match expr {
        Expr::Null => unreachable!(),

        Expr::Subselect(_, e) => group_nested(index, e),
        Expr::Expand(_, e, f) => {
            group_nested(index, e);
            if let Some(f) = f {
                group_nested(index, f);
            }
        }

        Expr::Not(e) => group_nested(index, e),

        Expr::WithList(v) => {
            let mut groups = HashMap::<String, Vec<Expr>>::new();

            // group sub-expressions in the WITH clause by their nested path
            while !v.is_empty() {
                let e = v.pop().unwrap();
                let nested_path = e.get_nested_path().expect("Expression is not nested");
                let nested_path = nested_path.split('.').next().unwrap();
                groups.entry(nested_path.into()).or_default().push(e);
            }

            // now, each of those are put in an Expr::AndList, and then wrapped in an Expr::Nested
            let mut ands = Vec::new();
            for (_, v) in groups.into_iter() {
                let mut e = Expr::WithList(v);
                maybe_nest(index, &mut e);
                ands.push(e);
            }

            if ands.len() == 1 {
                // only 1 thing, so swap out expr with whatever it is
                *expr = ands.pop().unwrap();
            } else {
                // and finally, we swap out expr with an Expr::AndList of all these
                *expr = Expr::WithList(ands);
            }
        }
        Expr::AndList(v) => v.iter_mut().for_each(|e| group_nested(index, e)),
        Expr::OrList(v) => v.iter_mut().for_each(|e| group_nested(index, e)),

        Expr::Linked(_, _) => unreachable!(),
        Expr::Nested(_, _) => {}
        Expr::Json(_) => {}

        Expr::Contains(_, _) => maybe_nest(index, expr),
        Expr::Eq(_, _) => maybe_nest(index, expr),
        Expr::Gt(_, _) => maybe_nest(index, expr),
        Expr::Lt(_, _) => maybe_nest(index, expr),
        Expr::Gte(_, _) => maybe_nest(index, expr),
        Expr::Lte(_, _) => maybe_nest(index, expr),
        Expr::Ne(_, _) => maybe_nest(index, expr),
        Expr::DoesNotContain(_, _) => maybe_nest(index, expr),
        Expr::Regex(_, _) => maybe_nest(index, expr),
        Expr::MoreLikeThis(_, _) => maybe_nest(index, expr),
        Expr::FuzzyLikeThis(_, _) => maybe_nest(index, expr),
        Expr::Matches(_, _) => maybe_nest(index, expr),
    }
}

fn maybe_nest(index: &Option<&PgRelation>, expr: &mut Expr) {
    // we can only do this if we have an index, which will maybe
    // only be None during standalone Rust #[test]s
    if let Some(index) = index {
        if let Some(path) = expr.get_nested_path() {
            let is_nested = is_nested_field(index, &path);
            if is_nested {
                // the nested query begins with the longest path if we have an index and it's a nested field
                *expr = Expr::Nested(path.clone(), Box::new(expr.clone()));
            }

            // then we build bottom-up for the leading paths
            let mut paths: Vec<&str> = path.split('.').collect();
            if is_nested || is_named_index_link(index, &path) {
                paths.pop();
            }

            while !paths.is_empty() {
                let path = paths.join(".");

                *expr = Expr::Nested(path, Box::new(expr.clone()));

                paths.pop();
            }
        }
    }
}
