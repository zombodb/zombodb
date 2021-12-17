use crate::zql::ast::{Expr, IndexLink};
use crate::zql::relationship_manager::RelationshipManager;

pub fn expand_index_links(
    expr: &mut Expr,
    root_index: &IndexLink,
    relationship_manager: &mut RelationshipManager,
) {
    expand_index_links0(expr, relationship_manager, &mut vec![root_index.clone()]);
}

fn expand_index_links0<'a>(
    expr: &'a mut Expr,
    relationship_manager: &mut RelationshipManager,
    target_stack: &mut Vec<IndexLink>,
) {
    match expr {
        Expr::Subselect(i, e) => {
            target_stack.push(i.clone());
            expand_index_links0(e, relationship_manager, target_stack);
            target_stack.pop();
        }
        Expr::Expand(i, e, f) => {
            target_stack.push(i.clone());
            if let Some(f) = f {
                expand_index_links0(f, relationship_manager, target_stack);
            }

            expand_index_links0(e, relationship_manager, target_stack);
            target_stack.pop();

            let target_index = i.clone();
            do_expand(expr, relationship_manager, target_stack, &target_index);
        }
        Expr::Not(e) => expand_index_links0(e, relationship_manager, target_stack),
        Expr::WithList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links0(e, relationship_manager, target_stack)),
        Expr::AndList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links0(e, relationship_manager, target_stack)),
        Expr::OrList(v) => v
            .iter_mut()
            .for_each(|e| expand_index_links0(e, relationship_manager, target_stack)),
        Expr::Nested(_, e) => expand_index_links0(e, relationship_manager, target_stack),

        Expr::Linked(i, e) => {
            expand_index_links0(e, relationship_manager, target_stack);
            let target_index = i.clone();
            do_expand(expr, relationship_manager, target_stack, &target_index);
        }
        _ => {}
    }
}

fn do_expand(
    expr: &mut Expr,
    relationship_manager: &mut RelationshipManager,
    target_stack: &mut Vec<IndexLink>,
    target_index: &IndexLink,
) {
    let current_index = target_stack.last().unwrap();
    let path = relationship_manager.calc_path(current_index, target_index);
    let mut target = target_index.clone();
    let mut left_field = target.left_field.clone();
    if left_field.is_some() && left_field.as_ref().unwrap().contains('.') {
        let mut parts = left_field.as_ref().unwrap().splitn(2, '.');
        parts.next();
        left_field = Some(parts.next().unwrap().to_string());
    }
    target.left_field = left_field;

    for link in path.into_iter().rev() {
        if target != link {
            let dummy = Expr::Null;
            let swapped = std::mem::replace(expr, dummy);
            *expr = Expr::Linked(link, Box::new(swapped));
        }
    }
}
