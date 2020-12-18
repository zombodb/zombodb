use crate::query_parser::ast::{Expr, IndexLink, QualifiedField};

pub fn find_fields(expr: &mut Expr, root_index: &IndexLink, indexes: &Vec<IndexLink>) {
    match expr {
        Expr::Null => unreachable!(),

        Expr::Subselect(i, e) => find_fields(e, i, indexes),
        Expr::Expand(_i, e, f) => {
            if let Some(filter) = f {
                find_fields(filter, root_index, indexes);
            }
            find_fields(e, root_index, indexes)
        }
        Expr::Nested(_, e) => find_fields(e, root_index, indexes),

        Expr::Not(r) => find_fields(r, root_index, indexes),
        Expr::WithList(v) | Expr::AndList(v) | Expr::OrList(v) => v
            .iter_mut()
            .for_each(|e| find_fields(e, root_index, indexes)),
        Expr::Linked(_, _) => {}
        Expr::Json(_) => {}
        Expr::Contains(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Eq(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Gt(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Lt(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Gte(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Lte(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Ne(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::DoesNotContain(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Regex(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::MoreLikeThis(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::FuzzyLikeThis(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
        Expr::Matches(f, _) => f.index = find_link_for_field(&f, root_index, indexes),
    }
}

pub fn find_link_for_field(
    field_name: &QualifiedField,
    root_index: &IndexLink,
    indexes: &Vec<IndexLink>,
) -> Option<IndexLink> {
    if field_name.index.is_some() {
        // we already know where the field lives
        return field_name.index.clone();
    }

    for mut index in Some(root_index).into_iter().chain(indexes.into_iter()) {
        if index.is_this_index() {
            // 'this.index', typically from an #expand or #subselect, means the original index
            index = root_index;
        }

        if index.name.is_some() && *index.name.as_ref().unwrap() == field_name.base_field() {
            // the base field name is the same as this named index link, so that's where it comes from
            return Some(index.clone());
        }

        let relation = if index == root_index {
            // if we can't open the root_index, that's okay, we'll just not do anything about that
            // this should only happen during our unit tests where we don't specify a valid table/index
            match index.open_table() {
                Ok(relation) => Some(relation),
                Err(_) => None,
            }
        } else {
            // but we definitely want to ensure we can open a linked index, and we'll panic!() if we can't
            Some(index.open_table().unwrap_or_else(|e| {
                panic!(
                    "failed to open linked index's table '{}': {}",
                    index.qualified_index.table_name(),
                    e
                )
            }))
        };

        if let Some(relation) = relation {
            for att in relation.tuple_desc().iter() {
                if att.name() == field_name.base_field() {
                    // the table behind this index link contains this field
                    return Some(index.clone());
                }
            }
        }
    }

    Some(root_index.clone())
}
