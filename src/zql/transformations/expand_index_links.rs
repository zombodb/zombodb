use crate::zql::ast::{Expr, IndexLink};
use crate::zql::relationship_manager::RelationshipManager;
// use indexmap::IndexMap;
use std::collections::HashMap as IndexMap;
pub fn expand_index_links(
    expr: &mut Expr,
    root_index: &IndexLink,
    relationship_manager: &mut RelationshipManager,
    indexes: &Vec<IndexLink>,
) {
    expand_index_links0(expr, root_index, relationship_manager, root_index, indexes);
}

fn expand_index_links0<'a>(
    expr: &'a mut Expr,
    root_index: &IndexLink,
    relationship_manager: &mut RelationshipManager,
    current_index: &IndexLink,
    indexes: &Vec<IndexLink>,
) {
    match expr {
        Expr::Subselect(i, e) => expand_index_links0(e, i, relationship_manager, i, indexes),
        Expr::Expand(i, e, f) => {
            if let Some(f) = f {
                expand_index_links0(f, root_index, relationship_manager, i, indexes);
            }

            expand_index_links0(e, root_index, relationship_manager, i, indexes);
        }
        Expr::Not(e) => {
            expand_index_links0(e, root_index, relationship_manager, current_index, indexes)
        }
        Expr::WithList(v) => v.iter_mut().for_each(|e| {
            expand_index_links0(e, root_index, relationship_manager, current_index, indexes)
        }),
        Expr::AndList(v) => v.iter_mut().for_each(|e| {
            expand_index_links0(e, root_index, relationship_manager, current_index, indexes)
        }),
        Expr::OrList(v) => v.iter_mut().for_each(|e| {
            expand_index_links0(e, root_index, relationship_manager, current_index, indexes)
        }),
        Expr::Nested(_, e) => {
            expand_index_links0(e, root_index, relationship_manager, current_index, indexes)
        }

        Expr::Linked(i, e) => {
            if current_index == i {
                expand_index_links0(e, root_index, relationship_manager, current_index, indexes)
            } else {
                let path = relationship_manager.calc_path(current_index, i);
                expand_index_links0(e, root_index, relationship_manager, current_index, indexes);
                let mut target = i.clone();
                let mut left_field = target.left_field.clone();
                if left_field.is_some() && left_field.as_ref().unwrap().contains('.') {
                    let mut parts = left_field.as_ref().unwrap().splitn(2, '.');
                    parts.next();
                    left_field = Some(parts.next().unwrap().to_string());
                }
                target.left_field = left_field;

                if path.is_empty() && indexes.contains(&target) {
                    *expr = *e.clone();
                } else {
                    for link in path.into_iter().rev() {
                        if target != link {
                            let dummy = Expr::Null;
                            let swapped = std::mem::replace(expr, dummy);
                            *expr = Expr::Linked(link, Box::new(swapped));
                        }
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
    groups: IndexMap<Option<IndexLink>, Vec<Expr<'a>>>,
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
            for expr in exprs {
                match expr {
                    Expr::Subselect(i, e) => {
                        v.push(Expr::Linked(i.clone(), Box::new(Expr::Subselect(i, e))));
                    }
                    _ => v.push(expr),
                }
            }
        }
    }
}

fn group_expressions_by_link<'a>(
    v: &mut Vec<Expr<'a>>,
) -> IndexMap<Option<IndexLink>, Vec<Expr<'a>>> {
    // next, lets group expressions by if they're Expr::Linked
    let mut groups = IndexMap::<Option<IndexLink>, Vec<Expr>>::new();
    while !v.is_empty() {
        let mut expr = v.pop().unwrap();
        merge_adjacent_links(&mut expr);
        match expr {
            Expr::Subselect(..) => groups.entry(None).or_default().push(expr),
            Expr::Linked(i, e) => groups.entry(Some(i)).or_default().push(*e),
            _ => groups.entry(None).or_default().push(expr),
        }
    }
    groups
}
