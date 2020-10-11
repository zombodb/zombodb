use crate::query_parser::ast::{Expr, IndexLink, QualifiedField};
use crate::query_parser::transformations::field_finder::find_link_for_field;
use std::collections::BTreeMap;

pub fn assign_links(
    root_index: &IndexLink,
    expr: &mut Expr,
    links: &Vec<IndexLink>,
) -> Option<IndexLink> {
    match expr {
        Expr::Subselect(i, e) => assign_links(&i, e, links),
        Expr::Expand(i, e, f) => {
            let mut other = e.clone();
            if let Some(filter) = f {
                assign_links(root_index, filter, links);
            }

            assign_links(root_index, e, links);

            let link = find_link_for_field(
                &QualifiedField {
                    index: None,
                    field: i.left_field.clone().unwrap(),
                },
                root_index,
                links,
            )
            .unwrap_or_else(|| panic!("failed to find linked field '{:?}'", i.left_field));

            let new_link = IndexLink {
                name: None,
                left_field: i.left_field.clone(),
                qualified_index: link.qualified_index.clone(),
                right_field: i.right_field.clone(),
            };

            assign_links(&new_link, other.as_mut(), links);

            *expr = Expr::OrList(vec![
                Expr::Expand(
                    i.clone(),
                    Box::new(Expr::Linked(new_link, e.clone())),
                    f.clone(),
                ),
                *other,
            ]);

            Some(link)
        }

        Expr::Not(e) => assign_links(root_index, e, links),

        Expr::WithList(v) => link_by_group(root_index, links, v, |exprs| Expr::WithList(exprs)),
        Expr::AndList(v) => link_by_group(root_index, links, v, |exprs| Expr::AndList(exprs)),
        Expr::OrList(v) => link_by_group(root_index, links, v, |exprs| Expr::OrList(exprs)),

        Expr::Linked(_, _) => unreachable!(),
        Expr::Nested(_, e) => assign_links(root_index, e, links),

        Expr::Json(_) => None,
        Expr::Contains(f, _) => f.index.clone(),
        Expr::Eq(f, _) => f.index.clone(),
        Expr::Gt(f, _) => f.index.clone(),
        Expr::Lt(f, _) => f.index.clone(),
        Expr::Gte(f, _) => f.index.clone(),
        Expr::Lte(f, _) => f.index.clone(),
        Expr::Ne(f, _) => f.index.clone(),
        Expr::DoesNotContain(f, _) => f.index.clone(),
        Expr::Regex(f, _) => f.index.clone(),
        Expr::MoreLikeThis(f, _) => f.index.clone(),
        Expr::FuzzyLikeThis(f, _) => f.index.clone(),
    }
}

fn link_by_group<F: Fn(Vec<Expr>) -> Expr>(
    root_index: &IndexLink,
    links: &Vec<IndexLink>,
    v: &mut Vec<Expr>,
    f: F,
) -> Option<IndexLink> {
    let mut groups = BTreeMap::<Option<IndexLink>, Vec<Expr>>::new();

    // group each Expr in 'v' together by its IndexLink
    // this happens by moving each Expr out of 'v' and into 'groups'
    //
    // we also want to ensure that 'v' itself stays live as we intend
    // to re-fill it at the end of this process
    while !v.is_empty() {
        let mut e = v.pop().unwrap();
        let link = assign_links(root_index, &mut e, links);

        // we .insert(0) instead of .push() to preserve the original order of the expressions
        groups.entry(link).or_default().insert(0, e);
    }

    if groups.len() == 1 {
        // they all belong to the same IndexLink, and we don't care which one that is
        let (k, mut exprs) = groups.into_iter().next().unwrap();

        // we need to add them back to this group's vec
        v.append(&mut exprs);
        return k;
    } else {
        // there's more than 1 IndexLink in use.
        //
        // each one that isn't the 'root_index'needs to be wrapped in an
        // Expr::Linked(_, Expr::XXXList(...)), using the provided function
        for (link, mut exprs) in groups.into_iter() {
            let link = link.unwrap();
            if root_index != &link {
                // this group's link isn't the root_link, so we need to wrap it
                let expr = f(exprs);
                let linked = Expr::Linked(link, Box::new(expr));
                v.push(linked);
            } else {
                // otherwise we can just append all the expressions in this group back to 'v'
                v.append(&mut exprs);
            }
        }
    }

    Some(root_index.clone())
}
