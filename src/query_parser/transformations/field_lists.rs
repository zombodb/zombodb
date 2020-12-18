use std::collections::HashMap;

use crate::query_parser::ast::{Expr, QualifiedField, Term};

pub fn expand_field_lists(e: &mut Expr, lists: &HashMap<String, Vec<QualifiedField>>) {
    match e {
        Expr::Null => unreachable!(),

        Expr::Subselect(_, e) => expand_field_lists(e.as_mut(), lists),
        Expr::Expand(_, e, f) => {
            if let Some(filter) = f {
                expand_field_lists(filter.as_mut(), lists);
            }
            expand_field_lists(e.as_mut(), lists)
        }

        Expr::Not(e) => expand_field_lists(e.as_mut(), lists),
        Expr::WithList(v) => v.iter_mut().for_each(|e| expand_field_lists(e, lists)),
        Expr::AndList(v) => v.iter_mut().for_each(|e| expand_field_lists(e, lists)),
        Expr::OrList(v) => v.iter_mut().for_each(|e| expand_field_lists(e, lists)),

        Expr::Linked(_, _) => unreachable!(),
        Expr::Nested(_, _) => unreachable!(),

        Expr::Json(_) => {}

        Expr::Contains(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Contains(f, t)) {
                *e = expr;
            }
        }
        Expr::Eq(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Eq(f, t)) {
                *e = expr;
            }
        }
        Expr::Gt(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Gt(f, t)) {
                *e = expr;
            }
        }

        Expr::Lt(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Lt(f, t)) {
                *e = expr;
            }
        }
        Expr::Gte(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Gte(f, t)) {
                *e = expr;
            }
        }
        Expr::Lte(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Lte(f, t)) {
                *e = expr;
            }
        }
        Expr::Ne(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Ne(f, t)) {
                *e = expr;
            }
        }
        Expr::DoesNotContain(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::DoesNotContain(f, t)) {
                *e = expr;
            }
        }
        Expr::Regex(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Regex(f, t)) {
                *e = expr;
            }
        }
        Expr::MoreLikeThis(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::MoreLikeThis(f, t)) {
                *e = expr;
            }
        }
        Expr::FuzzyLikeThis(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::FuzzyLikeThis(f, t)) {
                *e = expr;
            }
        }
        Expr::Matches(f, t) => {
            if let Some(expr) = make_or_list(f, t, &lists, |f, t| Expr::Matches(f, t)) {
                *e = expr;
            }
        }
    }
}

fn make_or_list<'a, F: Fn(QualifiedField, Term) -> Expr>(
    source_field: &QualifiedField,
    term: &Term<'a>,
    lists: &HashMap<String, Vec<QualifiedField>>,
    f: F,
) -> Option<Expr<'a>> {
    if let Some(fields) = lists.get(&source_field.field_name()) {
        let mut or_list = Vec::new();
        for field in fields {
            or_list.push(make_expr(field.clone(), term, &f))
        }

        if or_list.len() == 1 {
            or_list.pop()
        } else {
            Some(Expr::OrList(or_list))
        }
    } else {
        None
    }
}

fn make_expr<'a, F: Fn(QualifiedField, Term) -> Expr>(
    field: QualifiedField,
    term: &Term<'a>,
    f: &F,
) -> Expr<'a> {
    f(field, term.clone())
}
