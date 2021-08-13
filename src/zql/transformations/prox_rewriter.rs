use crate::zql::ast::{Expr, ProximityTerm, QualifiedField, Term};

pub fn rewrite_proximity_chains(expr: &mut Expr) {
    match expr {
        Expr::Null => unreachable!(),

        Expr::Subselect(_, e) => rewrite_proximity_chains(e.as_mut()),
        Expr::Expand(_, e, f) => {
            if let Some(filter) = f {
                rewrite_proximity_chains(filter.as_mut());
            }
            rewrite_proximity_chains(e.as_mut())
        }
        Expr::Not(e) => rewrite_proximity_chains(e.as_mut()),
        Expr::WithList(v) => v.iter_mut().for_each(|e| rewrite_proximity_chains(e)),
        Expr::AndList(v) => v.iter_mut().for_each(|e| rewrite_proximity_chains(e)),
        Expr::OrList(v) => v.iter_mut().for_each(|e| rewrite_proximity_chains(e)),

        Expr::Linked(_, e) => rewrite_proximity_chains(e.as_mut()),
        Expr::Nested(_, e) => rewrite_proximity_chains(e.as_mut()),

        Expr::Contains(f, t) => rewrite_term(f, t),
        Expr::Eq(f, t) => rewrite_term(f, t),
        Expr::DoesNotContain(f, t) => rewrite_term(f, t),
        Expr::Ne(f, t) => rewrite_term(f, t),

        Expr::Json(_) => {}
        Expr::Gt(_, _) => {}
        Expr::Lt(_, _) => {}
        Expr::Gte(_, _) => {}
        Expr::Lte(_, _) => {}
        Expr::Regex(_, _) => {}
        Expr::MoreLikeThis(_, _) => {}
        Expr::FuzzyLikeThis(_, _) => {}
        Expr::Matches(_, _) => {}
    }
}

fn rewrite_term(field: &QualifiedField, term: &mut Term) {
    match term {
        Term::ProximityChain(v) => {
            for part in v.iter_mut() {
                part.words
                    .iter_mut()
                    .for_each(|prox_term| rewrite_prox_term(field, prox_term));
            }
        }
        _ => {}
    }
}

fn rewrite_prox_term(field: &QualifiedField, prox_term: &mut ProximityTerm) {
    match prox_term {
        ProximityTerm::String(s, b) => {
            *prox_term = ProximityTerm::make_proximity_chain(field, &s, *b)
        }
        ProximityTerm::Wildcard(s, b) => {
            *prox_term = ProximityTerm::make_proximity_chain(field, &s, *b)
        }
        ProximityTerm::Fuzzy(s, fuzz, b) => {
            *prox_term = match ProximityTerm::make_proximity_chain(field, &s, *b) {
                ProximityTerm::String(s, b) => ProximityTerm::Fuzzy(s, *fuzz, b),
                _ => panic!("Fuzzy proximity value didn't parse correctly"),
            }
        }
        ProximityTerm::Phrase(s, b) => {
            *prox_term = ProximityTerm::make_proximity_chain(field, &s, *b)
        }
        ProximityTerm::Prefix(s, b) => {
            *prox_term = ProximityTerm::make_proximity_chain(field, &s, *b)
        }
        _ => {}
    }
}
