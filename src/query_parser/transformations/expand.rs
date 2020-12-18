use crate::query_parser::ast::{Expr, IndexLink, QualifiedField};
use crate::query_parser::transformations::field_finder::find_link_for_field;

pub(crate) fn expand(expr: &mut Expr, root_index: &IndexLink, indexes: &Vec<IndexLink>) {
    match expr {
        Expr::Null => {}
        Expr::Subselect(_, e) => expand(e, root_index, indexes),
        Expr::Expand(i, e, f) => {
            expand(e, root_index, indexes);
            if f.is_some() {
                expand(f.as_mut().unwrap(), root_index, indexes);
            }

            if i.is_this_index() {
                let rhs_link = find_link_for_field(
                    &QualifiedField {
                        index: None,
                        field: i.right_field.clone().unwrap(),
                    },
                    root_index,
                    indexes,
                )
                .unwrap_or_else(|| panic!("could not find rhs field for #expand link: {}", i));
                i.qualified_index = rhs_link.qualified_index;
            }

            if let Some(f) = f {
                *expr = Expr::OrList(vec![
                    *e.clone(),
                    Expr::AndList(vec![*f.clone(), expr.clone()]),
                ]);
            } else {
                *expr = Expr::OrList(vec![*e.clone(), expr.clone()]);
            }
        }
        Expr::Not(e) => expand(e, root_index, indexes),
        Expr::WithList(v) => v.iter_mut().for_each(|e| expand(e, root_index, indexes)),
        Expr::AndList(v) => v.iter_mut().for_each(|e| expand(e, root_index, indexes)),
        Expr::OrList(v) => v.iter_mut().for_each(|e| expand(e, root_index, indexes)),
        Expr::Linked(_, e) => expand(e, root_index, indexes),
        Expr::Nested(_, e) => expand(e, root_index, indexes),

        Expr::Json(_) => {}
        Expr::Contains(_, _) => {}
        Expr::Eq(_, _) => {}
        Expr::Gt(_, _) => {}
        Expr::Lt(_, _) => {}
        Expr::Gte(_, _) => {}
        Expr::Lte(_, _) => {}
        Expr::Ne(_, _) => {}
        Expr::DoesNotContain(_, _) => {}
        Expr::Regex(_, _) => {}
        Expr::MoreLikeThis(_, _) => {}
        Expr::FuzzyLikeThis(_, _) => {}
        Expr::Matches(_, _) => {}
    }
}
