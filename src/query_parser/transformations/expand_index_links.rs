use crate::query_parser::ast::{Expr, IndexLink, QualifiedField};
use crate::query_parser::transformations::field_finder::find_link_for_field;
use std::collections::HashMap;

pub fn expand_index_links(expr: &mut Expr, root_index: &IndexLink, index_links: &Vec<IndexLink>) {
    match expr {
        Expr::Subselect(i, e) => {
            // #subselect always wants to go to its link as the root
            expand_index_links(e, i, index_links)
        }
        Expr::Expand(_, e, f) => {
            // #expand always goes to the root index
            expand_index_links(e, root_index, index_links);
            if let Some(f) = f {
                expand_index_links(f, root_index, index_links);
            }
        }
        Expr::Not(e) => expand_index_links(e, root_index, index_links),
        Expr::WithList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links(e, root_index, index_links)),
        Expr::AndList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links(e, root_index, index_links)),
        Expr::OrList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links(e, root_index, index_links)),
        Expr::Nested(_, e) => expand_index_links(e, root_index, index_links),

        Expr::Linked(i, _) => {
            let root_index = Some(root_index.clone());
            let mut this_link = Some(i.clone());

            // if this Linked node isn't our root index we need to
            // find where its "left_field" actually lives
            while this_link.is_some() && this_link != root_index {
                let left_field = QualifiedField {
                    index: None,
                    field: this_link.unwrap().left_field.clone().unwrap(),
                };

                this_link =
                    find_link_for_field(&left_field, root_index.as_ref().unwrap(), index_links);

                if this_link.is_some() {
                    if root_index != this_link {
                        // and if its location also isn't our root index, we need to add another
                        // level of Linked indirection
                        *expr = Expr::Linked(
                            this_link.as_ref().unwrap().clone(),
                            Box::new(expr.clone()),
                        );
                    }
                }
            }
        }
        _ => {}
    }
}

pub fn merge_adjacent_links(expr: &mut Expr) {
    match expr {
        Expr::Subselect(_, e) => merge_adjacent_links(e),
        Expr::Expand(_, e, f) => {
            merge_adjacent_links(e);
            if let Some(f) = f {
                merge_adjacent_links(f);
            }
        }
        Expr::Not(e) => merge_adjacent_links(e),
        Expr::WithList(v) => {
            let groups = group_expressions_by_link(v);
            relink(|exprs| Expr::WithList(exprs), v, groups);
        }
        Expr::AndList(v) => {
            let groups = group_expressions_by_link(v);
            relink(|exprs| Expr::AndList(exprs), v, groups);
        }
        Expr::OrList(v) => {
            let groups = group_expressions_by_link(v);
            relink(|exprs| Expr::OrList(exprs), v, groups);
        }
        Expr::Nested(_, e) => merge_adjacent_links(e),

        Expr::Linked(_, e) => merge_adjacent_links(e),
        _ => {}
    }
}

fn relink<'a, F: Fn(Vec<Expr<'a>>) -> Expr<'a>>(
    f: F,
    v: &mut Vec<Expr<'a>>,
    groups: HashMap<Option<IndexLink>, Vec<Expr<'a>>>,
) {
    for (link, mut exprs) in groups {
        if let Some(link) = link {
            if exprs.len() == 1 {
                // it's just a standalone link
                v.push(Expr::Linked(link, Box::new(exprs.pop().unwrap())));
            } else {
                // it's a list of expressions that all belong to the same link
                v.push(Expr::Linked(link, Box::new(f(exprs))));
            }
        } else {
            // it a list of unlinked expressions, so just push them all back
            v.append(&mut exprs);
        }
    }
}

fn group_expressions_by_link<'a>(
    v: &mut Vec<Expr<'a>>,
) -> HashMap<Option<IndexLink>, Vec<Expr<'a>>> {
    // next, lets group expressions by if they're Expr::Linked
    let mut groups = HashMap::<Option<IndexLink>, Vec<Expr>>::new();
    while !v.is_empty() {
        let e = v.pop().unwrap();
        match e {
            Expr::Linked(i, e) => groups.entry(Some(i)).or_default().push(*e),
            _ => groups.entry(None).or_default().push(e),
        }
    }
    groups
}
