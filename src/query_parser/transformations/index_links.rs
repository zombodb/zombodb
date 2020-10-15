use crate::query_parser::ast::{Expr, IndexLink, QualifiedField};
use crate::query_parser::transformations::field_finder::find_link_for_field;
use std::collections::{BTreeMap, HashMap, HashSet};

pub fn assign_links<'a>(root_index: &IndexLink, expr: &mut Expr<'a>, indexes: &Vec<IndexLink>) {
    match determine_link(root_index, expr, indexes) {
        // everything belongs to the same link (that isn't the root_index), and whatever that is we wrapped it in an Expr::Linked
        Some(target_link) if &target_link != root_index => {
            let dummy = Expr::Null;
            let swapped = std::mem::replace(expr, dummy);
            *expr = Expr::Linked(target_link.clone(), Box::new(swapped));
        }

        // there's more than one link or it's the root_index and they've already been linked
        _ => {}
    }
}

fn determine_link(
    root_index: &IndexLink,
    expr: &mut Expr,
    indexes: &Vec<IndexLink>,
) -> Option<IndexLink> {
    match expr {
        Expr::Null => unreachable!(),

        Expr::Subselect(_, _) => unimplemented!("determine_link: subselect"),
        Expr::Expand(i, e, f) => {
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
                assign_links(i, f, indexes);
            }
            assign_links(i, e, indexes);

            pgx::info!("i={}", i);

            Some(i.clone())
        }

        Expr::Not(e) => determine_link(root_index, e, indexes),

        Expr::WithList(v) => group_links(root_index, v, indexes, |v| Expr::WithList(v)),
        Expr::AndList(v) => group_links(root_index, v, indexes, |v| Expr::AndList(v)),
        Expr::OrList(v) => group_links(root_index, v, indexes, |v| Expr::OrList(v)),

        Expr::Linked(_, _) => unreachable!("determine_link: linked"),

        Expr::Nested(_, e) => determine_link(root_index, e, indexes),

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
        | Expr::FuzzyLikeThis(f, _) => f.index.clone(),
    }
}

fn group_links<F: Fn(Vec<Expr>) -> Expr>(
    root_index: &IndexLink,
    v: &mut Vec<Expr>,
    indexes: &Vec<IndexLink>,
    f: F,
) -> Option<IndexLink> {
    // group the elements of 'v' together by whatever link we determine each to belong
    let mut link_groups = HashMap::<Option<IndexLink>, Vec<Expr>>::new();
    while !v.is_empty() {
        let mut e = v.pop().unwrap();
        link_groups
            .entry(determine_link(root_index, &mut e, indexes))
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
                Some(target_link) if &target_link != root_index => {
                    v.push(Expr::Linked(target_link, Box::new(expr_list)))
                }

                // it is the root_index, so we don't need to wrap it
                Some(_) => v.push(expr_list),

                // we should never get here
                None => unreachable!("None target_link"),
            }
        }
        None
    }
}
