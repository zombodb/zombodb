use crate::query_parser::ast::{Expr, IndexLink, QualifiedField};

// pub(crate) fn assign_indexes<'input>(
//     expr: &'input mut Expr<'input>,
//     root_index: &'input IndexLink<'input>,
//     indexes: &'input Vec<IndexLink<'input>>,
// ) {
pub(crate) fn find_fields(expr: &mut Expr, root_index: &IndexLink, indexes: &Vec<IndexLink>) {
    match expr {
        Expr::Subselect(i, e) | Expr::Expand(i, e) => {
            if i.is_this_index() {
                let (left, right) = (i.left_field.clone(), i.right_field.clone());
                *i = root_index.clone();
                i.left_field = left;
                i.right_field = right;
            }
            find_fields(e.as_mut(), i, indexes)
        }
        Expr::Not(r) => find_fields(r.as_mut(), root_index, indexes),
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
    }
}

// fn find_field<'input>(
//     field_name: &QualifiedField,
//     root_index: &'input IndexLink,
//     indexes: &Vec<IndexLink<'input>>,
// ) -> Option<IndexLink<'input>> {
fn find_link_for_field(
    field_name: &QualifiedField,
    root_index: &IndexLink,
    indexes: &Vec<IndexLink>,
) -> Option<IndexLink> {
    for mut index in Some(root_index).into_iter().chain(indexes.into_iter()) {
        if index.is_this_index() {
            // 'this.index', typically from an #expand or #subselect, means the original index
            index = root_index;
        }

        if index.name.is_some() && *index.name.as_ref().unwrap() == field_name.base_field() {
            // the base field name is the same as this named index link, so that's where it comes from
            return Some(index.clone());
        }

        let relation = index.open().unwrap_or_else(|_| {
            panic!(
                "no such relation from index options for qualified index: {}",
                index
            )
        });

        for att in relation.tuple_desc().iter() {
            if att.name() == field_name.base_field() {
                // the table behind this index link contains this field
                return Some(index.clone());
            }
        }
    }

    Some(root_index.clone())
}
