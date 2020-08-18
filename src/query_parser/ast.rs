use crate::query_parser::parser::{IndexLinkParser, Token};
use lalrpop_util::ParseError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Debug, Display, Error, Formatter};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProximityDistance {
    pub distance: u32,
    pub in_order: bool,
}

use crate::access_method::options::ZDBIndexOptions;
use crate::query_parser::optimizer::assign_indexes;
pub use pg_catalog::ProximityPart;
use pgx::PgRelation;

pub mod pg_catalog {
    use crate::query_parser::ast::{ProximityDistance, Term};
    use pgx::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, PostgresType, Serialize, Deserialize)]
    pub struct ProximityPart<'input> {
        #[serde(borrow)]
        pub words: Vec<Term<'input>>,
        pub distance: Option<ProximityDistance>,
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct QualifiedIndex {
    pub schema: Option<String>,
    pub table: String,
    pub index: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct QualifiedField<'input> {
    pub index: Option<QualifiedIndex>,
    pub field: &'input str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexLink<'input> {
    pub name: Option<&'input str>,
    pub left_field: &'input str,
    pub qualified_index: QualifiedIndex,
    pub right_field: &'input str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Opcode {
    Not,
    With,
    AndNot,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ComparisonOpcode {
    Contains,
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Ne,
    DoesNotContain,
    Regex,
    MoreLikeThis,
    FuzzyLikeThis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term<'input> {
    Null,
    String(&'input str, Option<f32>),
    Wildcard(&'input str, Option<f32>),
    Regex(&'input str, Option<f32>),
    Fuzzy(&'input str, u8, Option<f32>),
    Range(&'input str, &'input str, Option<f32>),
    ParsedArray(Vec<Term<'input>>, Option<f32>),
    UnparsedArray(&'input str, Option<f32>),

    ProximityChain(Vec<ProximityPart<'input>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'input> {
    Subselect(IndexLink<'input>, Box<Expr<'input>>),
    Expand(IndexLink<'input>, Box<Expr<'input>>),

    // types of connectors
    Not(Box<Expr<'input>>),
    With(Box<Expr<'input>>, Box<Expr<'input>>),
    And(Box<Expr<'input>>, Box<Expr<'input>>),
    Or(Box<Expr<'input>>, Box<Expr<'input>>),

    Linked(IndexLink<'input>, Vec<Expr<'input>>),

    // types of comparisons
    Json(String),
    Contains(QualifiedField<'input>, Term<'input>),
    Eq(QualifiedField<'input>, Term<'input>),
    Gt(QualifiedField<'input>, Term<'input>),
    Lt(QualifiedField<'input>, Term<'input>),
    Gte(QualifiedField<'input>, Term<'input>),
    Lte(QualifiedField<'input>, Term<'input>),
    Ne(QualifiedField<'input>, Term<'input>),
    DoesNotContain(QualifiedField<'input>, Term<'input>),
    Regex(QualifiedField<'input>, Term<'input>),
    MoreLikeThis(QualifiedField<'input>, Term<'input>),
    FuzzyLikeThis(QualifiedField<'input>, Term<'input>),
}

impl<'input> Term<'input> {
    pub(in crate::query_parser) fn maybe_make_wildcard_or_regex(
        opcode: Option<&ComparisonOpcode>,
        expr: Term<'input>,
    ) -> Term<'input> {
        match &expr {
            Term::String(s, b) => {
                if let Some(&ComparisonOpcode::Regex) = opcode {
                    Term::Regex(s.clone(), *b)
                } else {
                    let mut prev = 0 as char;
                    for c in s.chars() {
                        if (c == '*' || c == '?') && prev != '\\' {
                            return Term::Wildcard(s.clone(), *b);
                        }
                        prev = c;
                    }
                    expr
                }
            }
            _ => expr,
        }
    }
}

pub type ParserError<'input> = ParseError<usize, Token<'input>, &'static str>;

impl<'input> Expr<'input> {
    pub fn from_str(
        all_indexes: Vec<QualifiedIndex>,
        default_fieldname: &'input str,
        input: &'input str,
        used_fields: &mut HashSet<&'input str>,
    ) -> Result<Expr<'input>, ParserError<'input>> {
        let input = input.clone();
        let parser = crate::query_parser::parser::ExprParser::new();
        let mut operator_stack = vec![ComparisonOpcode::Contains];
        let mut fieldname_stack = vec![default_fieldname];

        let mut expr = parser.parse(
            used_fields,
            &mut fieldname_stack,
            &mut operator_stack,
            input,
        )?;

        let original_index = all_indexes.first().unwrap();
        assign_indexes(expr.as_mut(), original_index, original_index, &all_indexes);
        Ok(*expr)
    }

    pub(in crate::query_parser) fn from_opcode(
        field: &'input str,
        opcode: ComparisonOpcode,
        right: Term<'input>,
    ) -> Expr<'input> {
        let field_name = QualifiedField { index: None, field };

        match opcode {
            ComparisonOpcode::Contains => Expr::Contains(field_name, right),
            ComparisonOpcode::Eq => Expr::Eq(field_name, right),
            ComparisonOpcode::Gt => Expr::Gt(field_name, right),
            ComparisonOpcode::Lt => Expr::Lt(field_name, right),
            ComparisonOpcode::Gte => Expr::Gte(field_name, right),
            ComparisonOpcode::Lte => Expr::Lte(field_name, right),
            ComparisonOpcode::Ne => Expr::Ne(field_name, right),
            ComparisonOpcode::DoesNotContain => Expr::DoesNotContain(field_name, right),
            ComparisonOpcode::Regex => Expr::Regex(field_name, right),
            ComparisonOpcode::MoreLikeThis => Expr::MoreLikeThis(field_name, right),
            ComparisonOpcode::FuzzyLikeThis => Expr::FuzzyLikeThis(field_name, right),
        }
    }

    pub(in crate::query_parser) fn range_from_opcode(
        field: &'input str,
        opcode: ComparisonOpcode,
        start: &'input str,
        end: &'input str,
        boost: Option<f32>,
    ) -> Expr<'input> {
        let field_name = QualifiedField { index: None, field };

        let range = Term::Range(start, end, boost);
        match opcode {
            ComparisonOpcode::Contains => Expr::Contains(field_name, range),
            ComparisonOpcode::Eq => Expr::Eq(field_name, range),
            ComparisonOpcode::Ne => Expr::Ne(field_name, range),
            ComparisonOpcode::DoesNotContain => Expr::DoesNotContain(field_name, range),
            _ => panic!("invalid operator for range query"),
        }
    }

    pub(in crate::query_parser) fn extract_prox_terms(&self) -> Vec<Term<'input>> {
        let mut flat = Vec::new();
        match self {
            Expr::Or(l, r) => {
                flat.append(&mut l.extract_prox_terms());
                flat.append(&mut r.extract_prox_terms());
            }
            Expr::Contains(_, v) | Expr::Eq(_, v) | Expr::DoesNotContain(_, v) | Expr::Ne(_, v) => {
                match v {
                    Term::String(s, b) => {
                        flat.push(Term::String(s.clone(), *b));
                    }
                    Term::Wildcard(s, b) => {
                        flat.push(Term::Wildcard(s.clone(), *b));
                    }
                    Term::Fuzzy(s, d, b) => {
                        flat.push(Term::Fuzzy(s, *d, *b));
                    }
                    _ => panic!("Unsupported proximity group value: {}", self),
                }
            }
            _ => panic!("Unsupported proximity group value: {}", self),
        }
        flat
    }
}

impl Display for ProximityDistance {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        write!(
            fmt,
            " {}/{}",
            if self.in_order { "WO" } else { "W" },
            self.distance
        )
    }
}

impl QualifiedIndex {
    pub fn from_relation(index: &PgRelation) -> Self {
        QualifiedIndex {
            schema: Some(index.namespace().to_string()),
            table: index
                .heap_relation()
                .expect("specified relation is not an index")
                .name()
                .to_string(),
            index: index.name().to_string(),
        }
    }

    pub fn from_zdb(index: &PgRelation) -> Vec<Self> {
        let mut indexes = vec![QualifiedIndex::from_relation(index)];

        if let Some(links) = ZDBIndexOptions::from(index).links() {
            for link in links {
                let parser = IndexLinkParser::new();
                let mut used_fields = HashSet::new();
                let mut fieldname_stack = Vec::new();
                let mut operator_stack = Vec::new();
                let link = parser
                    .parse(
                        &mut used_fields,
                        &mut fieldname_stack,
                        &mut operator_stack,
                        link.as_str(),
                    )
                    .expect("failed to parse index link");
                indexes.push(link.qualified_index);
            }
        }

        indexes
    }

    pub fn table_name(&self) -> String {
        let mut relation_name = String::new();
        if let Some(schema) = &self.schema {
            relation_name.push_str(&schema);
            relation_name.push('.');
        }
        relation_name.push_str(&self.table);
        relation_name
    }

    pub fn is_this_index(&self) -> bool {
        self.schema.is_none() && self.table == "this" && self.index == "index"
    }
}

impl<'input> QualifiedField<'input> {
    pub fn base_field(&self) -> &str {
        match self.field.split('.').next() {
            Some(base) => base,
            None => self.field,
        }
    }
}

impl Display for QualifiedIndex {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        if let Some(schema) = self.schema.as_ref() {
            write!(fmt, "{}.{}.{}", schema, self.table, self.index)
        } else {
            write!(fmt, "{}.{}", self.table, self.index)
        }
    }
}

impl<'input> Display for QualifiedField<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        write!(fmt, "{}", self.field)
    }
}

impl<'input> Display for IndexLink<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        if let Some(name) = self.name {
            write!(fmt, "{}:(", name)?;
        }

        write!(
            fmt,
            "{}=<{}>{}",
            self.left_field, self.qualified_index, self.right_field
        )?;

        if let Some(_) = self.name {
            write!(fmt, ")")?;
        }

        Ok(())
    }
}

impl<'input> Display for Term<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            Term::Null => write!(fmt, "NULL"),

            Term::String(s, b) | Term::Wildcard(s, b) | Term::Regex(s, b) => {
                write!(fmt, "\"{}\"", s.replace('"', "\\\""))?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Term::Fuzzy(s, f, b) => {
                write!(fmt, "\"{}\"~{}", s.replace('"', "\\\""), f)?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Term::Range(s, e, b) => {
                write!(
                    fmt,
                    "\"{}\" /TO/ \"{}\"",
                    s.replace('"', "\\\""),
                    e.replace('"', "\\\""),
                )?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Term::ParsedArray(a, b) => {
                write!(fmt, "[")?;
                for (i, elem) in a.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, ",")?;
                    }
                    write!(fmt, "{}", elem)?;
                }
                write!(fmt, "]")?;

                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Term::UnparsedArray(a, b) => {
                write!(fmt, "[[{}]]", a)?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Term::ProximityChain(parts) => {
                write!(fmt, "(")?;
                let mut iter = parts.iter().peekable();
                while let Some(part) = iter.next() {
                    let next = iter.peek();

                    if part.words.len() == 1 {
                        let word = part.words.get(0).unwrap();
                        match next {
                            Some(_) => write!(fmt, "{}{} ", word, part.distance.as_ref().unwrap())?,
                            None => write!(fmt, "{}", word)?,
                        };
                    } else {
                        write!(fmt, "(")?;
                        for (idx, word) in part.words.iter().enumerate() {
                            if idx > 0 {
                                write!(fmt, ",")?;
                            }
                            write!(fmt, "{}", word)?;
                        }
                        write!(fmt, ")")?;
                        match next {
                            Some(_) => {
                                write!(fmt, "{} ", part.distance.as_ref().unwrap())?;
                            }
                            None => {}
                        }
                    }
                }
                write!(fmt, ")")
            }
        }
    }
}

impl<'input> Display for Expr<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            Expr::Subselect(link, q) => write!(fmt, "#subselect<{}>({})", link, q),
            Expr::Expand(link, q) => write!(fmt, "#expand<{}>({})", link, q),

            Expr::Not(r) => write!(fmt, "NOT ({})", r),
            Expr::With(l, r) => write!(fmt, "({} WITH {})", l, r),
            Expr::And(l, r) => write!(fmt, "({} AND {})", l, r),
            Expr::Or(l, r) => write!(fmt, "({} OR {})", l, r),

            Expr::Linked(_, v) => {
                write!(fmt, "(")?;
                for e in v {
                    write!(fmt, "{}", e)?;
                }
                write!(fmt, ")")
            }

            Expr::Json(s) => write!(fmt, "({})", s),

            Expr::Contains(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Contains, r),
            Expr::Eq(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Eq, r),
            Expr::Gt(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Gt, r),
            Expr::Lt(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Lt, r),
            Expr::Gte(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Gte, r),
            Expr::Lte(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Lte, r),
            Expr::Ne(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Ne, r),
            Expr::DoesNotContain(l, r) => {
                write!(fmt, "{}{}{}", l, ComparisonOpcode::DoesNotContain, r)
            }
            Expr::Regex(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Regex, r),
            Expr::MoreLikeThis(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::MoreLikeThis, r),
            Expr::FuzzyLikeThis(l, r) => {
                write!(fmt, "{}{}{}", l, ComparisonOpcode::FuzzyLikeThis, r)
            }
        }
    }
}

impl Display for Opcode {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        use self::Opcode::*;
        match *self {
            Not => write!(fmt, "NOT"),
            With => write!(fmt, "WITH"),
            AndNot => write!(fmt, "AND NOT"),
            And => write!(fmt, "AND"),
            Or => write!(fmt, "OR"),
        }
    }
}

impl Display for ComparisonOpcode {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), Error> {
        use self::ComparisonOpcode::*;
        match *self {
            Contains => write!(fmt, ":"),
            Eq => write!(fmt, "="),
            Gt => write!(fmt, ">"),
            Lt => write!(fmt, "<"),
            Gte => write!(fmt, ">="),
            Lte => write!(fmt, "<="),
            Ne => write!(fmt, "!="),
            DoesNotContain => write!(fmt, "<>"),
            Regex => write!(fmt, ":~"),
            MoreLikeThis => write!(fmt, ":@"),
            FuzzyLikeThis => write!(fmt, ":@~"),
        }
    }
}

// fn group_links(expr: Expr, current_index: QualifiedIndex) -> (QualifiedIndex, Expr) {
//     match expr {
//         Expr::Subselect(i, e) => (i.qualified_index.clone(), Expr::Subselect(i, e)),
//         Expr::Expand(i, e) => (i.qualified_index.clone(), Expr::Expand(i, e)),
//
//         Expr::Not(r) => {
//             let (current_index, r) = group_links(*r, current_index.clone());
//             (current_index, Expr::Not(Box::new(r)))
//         }
//         Expr::With(l, r) => {
//             let (i, l, r) = maybe_link(*l, *r, current_index.clone());
//             (i, Expr::With(Box::new(l), Box::new(r)))
//         }
//         Expr::And(l, r) => {
//             let (i, l, r) = maybe_link(*l, *r, current_index.clone());
//             (i, Expr::And(Box::new(l), Box::new(r)))
//         }
//         Expr::Or(l, r) => {
//             let (i, l, r) = maybe_link(*l, *r, current_index.clone());
//             (i, Expr::Or(Box::new(l), Box::new(r)))
//         }
//         Expr::Linked(i, v) => (i.qualified_index.clone(), Expr::Linked(i, v)),
//
//         Expr::Json(_) => (current_index, expr),
//         Expr::Range(_, _, _) => (current_index, expr),
//         Expr::Contains(f, t) => (f.index.clone(), Expr::Contains(f, t)),
//         Expr::Eq(f, t) => (f.index.clone(), Expr::Eq(f, t)),
//         Expr::Gt(f, t) => (f.index.clone(), Expr::Gt(f, t)),
//         Expr::Lt(f, t) => (f.index.clone(), Expr::Lt(f, t)),
//         Expr::Gte(f, t) => (f.index.clone(), Expr::Gte(f, t)),
//         Expr::Lte(f, t) => (f.index.clone(), Expr::Lte(f, t)),
//         Expr::Ne(f, t) => (f.index.clone(), Expr::Ne(f, t)),
//         Expr::DoesNotContain(f, t) => (f.index.clone(), Expr::DoesNotContain(f, t)),
//         Expr::Regex(f, t) => (f.index.clone(), Expr::Regex(f, t)),
//         Expr::MoreLikeThis(f, t) => (f.index.clone(), Expr::MoreLikeThis(f, t)),
//         Expr::FuzzyLikeThis(f, t) => (f.index.clone(), Expr::FuzzyLikeThis(f, t)),
//     }
// }
//
// fn maybe_link<'input>(
//     mut l: Expr<'input>,
//     mut r: Expr<'input>,
//     current_index: QualifiedIndex,
// ) -> (QualifiedIndex, Expr<'input>, Expr<'input>) {
//     let (left_index, left) = group_links(l, current_index.clone());
//     let (right_index, right) = group_links(r, current_index.clone());
//     let mut new_index = current_index;
//
//     if left_index == right_index {
//         // indexes are the same, so we'll just bubble one of them up with the Expr
//         new_index = left_index;
//     } else {
//         // they're not the same, so we must wrap l & r in individual Nested expressions
//         l = Expr::Linked(find_index_link(left_index), vec![left]);
//         r = Expr::Linked(find_index_link(right_index), vec![right]);
//     }
//
//     ((), (), ())
// }
//
// fn find_index_link<'input>(index: QualifiedIndex) -> IndexLink<'input> {
//     // TODO:  actually implement this
//     IndexLink {
//         name: None,
//         left_field: "",
//         qualified_index: index,
//         right_field: "",
//     }
// }
