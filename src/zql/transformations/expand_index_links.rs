use crate::zql::ast::{Expr, IndexLink};
use crate::zql::relationship_manager::RelationshipManager;

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
