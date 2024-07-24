use crate::zql::ast::Expr;

pub fn pullup_and(expr: &mut Expr) {
    loop {
        if !pullup_and0(expr) {
            break;
        }
    }
}

fn pullup_and0(expr: &mut Expr) -> bool {
    let mut changed = false;

    match expr {
        Expr::AndList(list) => {
            let mut tmp = Vec::with_capacity(list.len());

            for expr in list.drain(..) {
                if let Expr::AndList(inner) = expr {
                    tmp.extend(inner);
                    changed = true;
                } else {
                    tmp.push(expr);
                }
            }

            *list = tmp
        }

        Expr::OrList(list) => {
            for expr in list {
                changed |= pullup_and0(expr);
            }
        }

        Expr::WithList(list) => {
            for expr in list {
                changed |= pullup_and0(expr);
            }
        }

        _ => {}
    }

    return changed;
}
