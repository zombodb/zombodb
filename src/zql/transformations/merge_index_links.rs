use crate::zql::ast::{Expr, IndexLink};
use std::collections::HashMap;

pub fn merge_adjacent_links(expr: &mut Expr) -> bool {
    let mut changed = false;
    match expr {
        Expr::Null => {}
        Expr::Not(e) => {
            changed |= merge_adjacent_links(e);
            if e.is_linked() {
                let dummy = Box::new(Expr::Null);
                let swapped = std::mem::replace(e, dummy);
                if let Expr::Linked(link, inner) = *swapped {
                    *expr = Expr::Linked(link, Box::new(Expr::Not(inner)));
                } else {
                    unreachable!("it wasn't an Expr::Linked")
                }
            }
        }
        Expr::WithList(v) => {
            changed |= do_merge(v, |members| Expr::WithList(members));
            if v.len() == 1 {
                *expr = v.pop().unwrap();
            }
        }
        Expr::AndList(v) => {
            changed |= do_merge(v, |members| Expr::AndList(members));
            if v.len() == 1 {
                *expr = v.pop().unwrap();
            }
        }
        Expr::OrList(v) => {
            changed |= do_merge(v, |members| Expr::OrList(members));
            if v.len() == 1 {
                *expr = v.pop().unwrap();
            }
        }
        Expr::Linked(_, e) => changed |= merge_adjacent_links(e),
        Expr::Nested(_, e) => changed |= merge_adjacent_links(e),
        Expr::Subselect(_, e) => changed |= merge_adjacent_links(e),
        Expr::Expand(_, e, f) => {
            changed |= merge_adjacent_links(e);
            if f.is_some() {
                changed |= merge_adjacent_links(f.as_deref_mut().unwrap());
            }
        }
        Expr::Json(_) => {}
        Expr::Contains(_, _) => {}
        Expr::Eq(_, _) => {}
        Expr::Gt(_, _) => {}
        Expr::Lt(_, _) => {}
        Expr::Gte(_, _) => {}
        Expr::Lte(_, _) => {}
        Expr::Ne(_, _) => {}
        Expr::DoesNotContain(_, _) => {}
        Expr::Regex(_, _) => {}
        Expr::MoreLikeThis(_, _) => {}
        Expr::FuzzyLikeThis(_, _) => {}
        Expr::Matches(_, _) => {}
    }

    if changed {
        changed = merge_adjacent_links(expr);
    }
    changed
}

fn do_merge<F: Fn(Vec<Expr>) -> Expr>(v: &mut Vec<Expr>, group_func: F) -> bool {
    let many = v.len();
    let mut changed = false;

    // group the elements of `v` together by their Some(IndexLink) or None
    let mut groups = HashMap::<Option<IndexLink>, Vec<Expr>>::new();
    for mut e in v.drain(..) {
        changed |= merge_adjacent_links(&mut e);

        let (link, inner) = if let Expr::Linked(link, inner) = e {
            (Some(link), *inner)
        } else {
            (None, e)
        };

        groups.entry(link).or_default().push(inner);
    }

    for (link, mut members) in groups {
        match link {
            Some(link) => {
                if members.len() == 1 {
                    v.push(Expr::Linked(link, Box::new(members.pop().unwrap())));
                } else {
                    v.push(Expr::Linked(link, Box::new(group_func(members))));
                }
            }
            None => v.extend(members),
        }
    }

    changed |= v.len() != many;

    changed
}
