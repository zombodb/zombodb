use crate::query_parser::ast::{Expr, IndexLink};
use crate::query_parser::transformations::dijkstra::RelationshipManager;
use std::collections::HashMap;

pub fn expand_index_links(
    expr: &mut Expr,
    root_index: &IndexLink,
    relationship_manager: &mut RelationshipManager,
) {
    expand_index_links0(expr, root_index, relationship_manager, root_index);
}

fn expand_index_links0<'a>(
    expr: &'a mut Expr,
    root_index: &IndexLink,
    relationship_manager: &mut RelationshipManager,
    current_index: &IndexLink,
) {
    match expr {
        Expr::Subselect(_i, _e) => unimplemented!("expand_index_links: #subselect"),
        Expr::Expand(i, e, f) => {
            if let Some(f) = f {
                expand_index_links0(f, root_index, relationship_manager, i);
            }

            expand_index_links0(e, root_index, relationship_manager, i);
        }
        Expr::Not(e) => expand_index_links0(e, root_index, relationship_manager, current_index),
        Expr::WithList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links0(e, root_index, relationship_manager, current_index)),
        Expr::AndList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links0(e, root_index, relationship_manager, current_index)),
        Expr::OrList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links0(e, root_index, relationship_manager, current_index)),
        Expr::Nested(_, e) => {
            expand_index_links0(e, root_index, relationship_manager, current_index)
        }

        Expr::Linked(i, e) => {
            let path = relationship_manager.calc_path(current_index, i);
            // pgx::info!("\nfrom: {}\n  to: {}\npath:{:#?}", current_index, i, path);

            if let Some(current_index) = path.last() {
                expand_index_links0(e, root_index, relationship_manager, current_index);
                let mut target = i.clone();
                let mut left_field = target.left_field.clone().expect("no left field");
                if left_field.contains('.') {
                    let mut parts = left_field.splitn(2, '.');
                    parts.next();
                    left_field = parts.next().unwrap().to_string();
                }
                target.left_field = Some(left_field);

                for link in path.into_iter().rev() {
                    if target != link {
                        let dummy = Expr::Null;
                        let swapped = std::mem::replace(expr, dummy);
                        *expr = Expr::Linked(link, Box::new(swapped));
                    }
                }
            } else {
                *expr = *e.clone();
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
