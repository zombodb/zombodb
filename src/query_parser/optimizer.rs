use crate::query_parser::ast::{Expr, QualifiedField, QualifiedIndex};
use pgx::*;

pub(crate) fn assign_indexes(
    expr: &mut Expr,
    original_index: &QualifiedIndex,
    root_index: &QualifiedIndex,
    indexes: &Vec<QualifiedIndex>,
) {
    match expr {
        Expr::Subselect(i, e) => {
            if i.qualified_index.is_this_index() {
                i.qualified_index = original_index.clone();
            }
            assign_indexes(e.as_mut(), original_index, &i.qualified_index, indexes)
        }
        Expr::Expand(i, e) => {
            if i.qualified_index.is_this_index() {
                i.qualified_index = original_index.clone();
            }
            assign_indexes(e.as_mut(), original_index, &i.qualified_index, indexes)
        }
        Expr::Not(r) => assign_indexes(r.as_mut(), original_index, root_index, indexes),
        Expr::With(l, r) => {
            assign_indexes(l.as_mut(), original_index, root_index, indexes);
            assign_indexes(r.as_mut(), original_index, root_index, indexes);
        }
        Expr::And(l, r) => {
            assign_indexes(l.as_mut(), original_index, root_index, indexes);
            assign_indexes(r.as_mut(), original_index, root_index, indexes);
        }
        Expr::Or(l, r) => {
            assign_indexes(l.as_mut(), original_index, root_index, indexes);
            assign_indexes(r.as_mut(), original_index, root_index, indexes);
        }
        Expr::Linked(_, _) => {}
        Expr::Json(_) => {}
        Expr::Contains(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Eq(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Gt(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Lt(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Gte(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Lte(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Ne(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::DoesNotContain(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::Regex(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::MoreLikeThis(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
        Expr::FuzzyLikeThis(f, _) => f.index = find_field(&f, original_index, root_index, indexes),
    }
}

fn find_field(
    field_name: &QualifiedField,
    original_index: &QualifiedIndex,
    root_index: &QualifiedIndex,
    indexes: &Vec<QualifiedIndex>,
) -> Option<QualifiedIndex> {
    for mut index in Some(root_index).into_iter().chain(indexes.into_iter()) {
        if index.is_this_index() {
            // 'this.index', typically from an #expand or #subselect, means the original index
            index = original_index;
        }

        let relation = PgRelation::open_with_name_and_share_lock(&index.table_name())
            .unwrap_or_else(|_| {
                panic!(
                    "no such relation from index options for qualified index: {}",
                    index
                )
            });

        for att in relation.tuple_desc().iter() {
            if att.name() == field_name.base_field() {
                return Some(QualifiedIndex {
                    schema: Some(relation.namespace().to_string()),
                    table: index.table.clone(),
                    index: index.index.clone(),
                });
            }
        }
    }

    None
}
