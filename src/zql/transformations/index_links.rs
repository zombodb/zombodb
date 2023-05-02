use crate::zql::ast::{Expr, IndexLink, QualifiedField};
use crate::zql::transformations::field_finder::find_link_for_field;
use indexmap::IndexMap;

pub fn assign_links<'a>(
    original_index: &IndexLink,
    root_index: &IndexLink,
    expr: &mut Expr<'a>,
    indexes: &Vec<IndexLink>,
) {
    match determine_link(original_index, root_index, expr, indexes) {
        // everything belongs to the same link (that isn't the root_index), and whatever that is we wrapped it in an Expr::Linked
        Some(target_link) if &target_link.qualified_index != &root_index.qualified_index => {
            match expr {
                Expr::Not(inner) => {
                    let dummy = Box::new(Expr::Null);
                    let swapped = std::mem::replace(inner, dummy);
                    *expr = Expr::Not(Box::new(Expr::Linked(target_link, swapped)))
                }
                _ => {
                    let dummy = Expr::Null;
                    let swapped = std::mem::replace(expr, dummy);
                    *expr = Expr::Linked(target_link, Box::new(swapped));
                }
            }
        }

        // there's more than one link or it's the root_index and they've already been linked
        _ => {}
    }
}

fn determine_link(
    original_index: &IndexLink,
    root_index: &IndexLink,
    expr: &mut Expr,
    indexes: &Vec<IndexLink>,
) -> Option<IndexLink> {
    match expr {
        Expr::Null => unreachable!(),

        Expr::Subselect(i, e) => {
            determine_link(original_index, i, e, indexes);
            None
        }
        Expr::Expand(i, e, f) => {
            if let Some(f) = f {
                determine_link(original_index, i, f, indexes);
            }
            determine_link(original_index, i, e, indexes);
            None
        }

        Expr::Not(e) => {
            determine_link(original_index, root_index, e.as_mut(), indexes);
            None
        }

        Expr::WithList(v) => group_links(original_index, root_index, v, indexes, |v| {
            Expr::WithList(v)
        }),
        Expr::AndList(v) => {
            group_links(original_index, root_index, v, indexes, |v| Expr::AndList(v))
        }
        Expr::OrList(v) => group_links(original_index, root_index, v, indexes, |v| Expr::OrList(v)),

        Expr::Linked(_, _) => unreachable!("determine_link: linked"),

        Expr::Nested(path, _) => {
            let mut index_link = find_link_for_field(
                &QualifiedField {
                    index: None,
                    field: path.to_string(),
                },
                root_index,
                indexes,
            )
            .unwrap();

            if !index_link
                .contains_field(path.split('.').next().unwrap())
                .unwrap_or(false)
            {
                index_link = find_link_for_field(
                    &QualifiedField {
                        index: None,
                        field: path.to_string(),
                    },
                    original_index,
                    indexes,
                )
                .unwrap();
            }

            maybe_link(root_index, expr, index_link)
        }

        Expr::Json(_) => Some(root_index.clone()),

        Expr::Contains(f, _)
        | Expr::Eq(f, _)
        | Expr::Gt(f, _)
        | Expr::Lt(f, _)
        | Expr::Gte(f, _)
        | Expr::Lte(f, _)
        | Expr::Ne(f, _)
        | Expr::DoesNotContain(f, _)
        | Expr::Regex(f, _)
        | Expr::MoreLikeThis(f, _)
        | Expr::FuzzyLikeThis(f, _)
        | Expr::Matches(f, _) => {
            let index_link = f.index.clone().unwrap();
            maybe_link(root_index, expr, index_link)
        }
    }
}

fn maybe_link(root_index: &IndexLink, expr: &mut Expr, index_link: IndexLink) -> Option<IndexLink> {
    if root_index.qualified_index != index_link.qualified_index {
        *expr = Expr::Linked(index_link, Box::new(expr.clone()));
        Some(root_index.clone())
    } else {
        Some(index_link)
    }
}

fn group_links<F: Fn(Vec<Expr>) -> Expr>(
    original_index: &IndexLink,
    root_index: &IndexLink,
    v: &mut Vec<Expr>,
    indexes: &Vec<IndexLink>,
    f: F,
) -> Option<IndexLink> {
    // group the elements of 'v' together by whatever link we determine each to belong
    let mut link_groups = IndexMap::<Option<IndexLink>, Vec<Expr>>::new();
    for mut e in v.drain(..) {
        link_groups
            .entry(determine_link(original_index, root_index, &mut e, indexes))
            .or_default()
            .push(e);
    }

    if link_groups.len() == 1 {
        // there is only one link, so we just return that after adding everything back into 'v'
        let (target_link, mut exprs) = link_groups.into_iter().next().unwrap();
        v.append(&mut exprs);
        target_link
    } else {
        // there's multiple links, so go ahead and group those together
        for (target_link, mut exprs) in link_groups {
            let expr_list = if exprs.len() == 1 {
                exprs.pop().unwrap()
            } else {
                f(exprs)
            };

            match target_link {
                // the link is not the root_index, so we must wrap it in an Expr::Linked
                Some(target_link)
                    if &target_link.qualified_index != &root_index.qualified_index =>
                {
                    if let Expr::Not(not_expr) = expr_list {
                        v.push(Expr::Not(Box::new(Expr::Linked(target_link, not_expr))))
                    } else {
                        v.push(Expr::Linked(target_link, Box::new(expr_list)))
                    }
                }

                // it is the root_index, so we don't need to wrap it
                Some(_) => v.push(expr_list),

                // we might get here if there's an #expand<> in our query tree
                None => v.push(expr_list),
            }
        }
        None
    }
}
