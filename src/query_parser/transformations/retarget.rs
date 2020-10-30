use crate::query_parser::ast::{Expr, IndexLink};
use crate::query_parser::relationship_manager::RelationshipManager;

pub(crate) fn retarget_expr<'a>(
    mut expr: Expr<'a>,
    source_link: &IndexLink,
    target_link: &Option<IndexLink>,
    relationship_manager: &mut RelationshipManager,
) -> Expr<'a> {
    if let Some(target_link) = target_link {
        let mut path = relationship_manager.calc_path(target_link, source_link);
        if path.is_empty() {
            panic!("no path fro {} to {}", target_link, source_link);
        }

        while let Some(link) = path.pop() {
            expr = Expr::Linked(link, Box::new(expr));
        }
    }

    expr
}
