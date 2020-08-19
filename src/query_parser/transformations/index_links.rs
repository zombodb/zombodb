use crate::query_parser::ast::{Expr, IndexLink, QualifiedIndex};

pub fn assign_links(
    root_index: &IndexLink,
    expr: &mut Expr,
    links: &Vec<IndexLink>,
) -> Option<IndexLink> {
    match expr {
        Expr::Subselect(i, e) => assign_links(&i, e, links),
        Expr::Expand(i, e) => assign_links(&i, e, links),

        Expr::Not(_) => unimplemented!(),
        // Expr::With(l, r) | Expr::And(l, r) | Expr::Or(l, r) => {
        //     let li = assign_links(root_index, l.as_mut(), links);
        //     let ri = assign_links(root_index, r.as_mut(), links);
        //
        //     if li.eq(&ri) {
        //         li
        //     } else {
        //         let li = li.unwrap();
        //         let ri = ri.unwrap();
        //
        //         if li.ne(root_index) {
        //             let l_linked = Expr::Linked(li.clone(), Box::new(*l.clone()));
        //             let _ = std::mem::replace(l, Box::new(l_linked));
        //         }
        //
        //         if ri.ne(root_index) {
        //             let r_linked = Expr::Linked(li.clone(), Box::new(*r.clone()));
        //             let _ = std::mem::replace(r, Box::new(r_linked));
        //         }
        //
        //         Some(root_index.clone())
        //     }
        // }
        Expr::WithList(v) => Some(root_index.clone()),
        Expr::AndList(v) => Some(root_index.clone()),
        Expr::OrList(v) => Some(root_index.clone()),

        Expr::Linked(_, _) => unreachable!(),

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
