use crate::query_parser::parser::Token;
use lalrpop_util::ParseError;
use std::fmt::{Debug, Display, Error, Formatter};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProximityDistance {
    pub distance: u32,
    pub in_order: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProximityPart<'input> {
    pub words: Vec<Term<'input>>,
    pub distance: Option<ProximityDistance>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QualifiedIndex {
    pub schema: Option<String>,
    pub table: String,
    pub index: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QualifiedField {
    pub index: QualifiedIndex,
    pub field: String,
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

#[derive(Debug, Clone, PartialEq)]
pub enum Term<'input> {
    Null,
    String(&'input str, Option<f32>),
    Wildcard(&'input str, Option<f32>),
    Fuzzy(&'input str, u8, Option<f32>),
    ParsedArray(Vec<Term<'input>>, Option<f32>),
    UnparsedArray(&'input str, Option<f32>),
    Range(&'input str, &'input str, Option<f32>),

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

    // types of comparisons
    Json(String),
    Contains(QualifiedField, Term<'input>),
    Eq(QualifiedField, Term<'input>),
    Gt(QualifiedField, Term<'input>),
    Lt(QualifiedField, Term<'input>),
    Gte(QualifiedField, Term<'input>),
    Lte(QualifiedField, Term<'input>),
    Ne(QualifiedField, Term<'input>),
    DoesNotContain(QualifiedField, Term<'input>),
    Regex(QualifiedField, Term<'input>),
    MoreLikeThis(QualifiedField, Term<'input>),
    FuzzyLikeThis(QualifiedField, Term<'input>),
}

impl<'input> Term<'input> {
    pub(in crate::query_parser) fn maybe_make_wildcard(expr: Term<'input>) -> Term<'input> {
        match expr {
            Term::String(s, b) => {
                let mut prev = 0 as char;
                for c in s.chars() {
                    if (c == '*' || c == '?') && prev != '\\' {
                        return Term::Wildcard(s, b);
                    }
                    prev = c;
                }
                expr
            }
            _ => expr,
        }
    }
}

pub type ParserError<'input> = ParseError<usize, Token<'input>, &'static str>;

impl<'input> Expr<'input> {
    pub fn from_str(
        default_index: QualifiedIndex,
        default_fieldname: &'input str,
        input: &'input str,
    ) -> Result<Box<Expr<'input>>, ParserError<'input>> {
        let input = input.clone();
        let parser = crate::query_parser::parser::ExprParser::new();
        let mut index_stack = vec![default_index];
        let mut operator_stack = vec![ComparisonOpcode::Contains];
        let mut fieldname_stack = vec![default_fieldname];

        parser.parse(
            &mut fieldname_stack,
            &mut operator_stack,
            &mut index_stack,
            input,
        )
    }

    pub(in crate::query_parser) fn from_opcode(
        index_stack: &Vec<QualifiedIndex>,
        field: &'input str,
        opcode: ComparisonOpcode,
        right: Term<'input>,
    ) -> Expr<'input> {
        let index = *index_stack.last().as_ref().unwrap();
        let field_name = QualifiedField {
            index: index.clone(),
            field: field.to_string(),
        };

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
                        flat.push(Term::String(s, *b));
                    }
                    Term::Wildcard(s, b) => {
                        flat.push(Term::Wildcard(s, *b));
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

impl Display for QualifiedIndex {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        if let Some(schema) = self.schema.as_ref() {
            write!(fmt, "{}.{}.{}", schema, self.table, self.index)
        } else {
            write!(fmt, "{}.{}", self.table, self.index)
        }
    }
}

impl Display for QualifiedField {
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

            Term::String(s, b) | Term::Wildcard(s, b) => {
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

            Term::Range(start, end, b) => {
                write!(
                    fmt,
                    "\"{}\" /TO/ \"{}\"",
                    start.replace('"', "\\\""),
                    end.replace('"', "\\\"")
                )?;

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
            Expr::Subselect(ref link, ref q) => write!(fmt, "#subselect<{}>({})", link, q),
            Expr::Expand(ref link, ref q) => write!(fmt, "#expand<{}>({})", link, q),

            Expr::Not(ref r) => write!(fmt, "NOT ({})", r),
            Expr::With(ref l, ref r) => write!(fmt, "({} WITH {})", l, r),
            Expr::And(ref l, ref r) => write!(fmt, "({} AND {})", l, r),
            Expr::Or(ref l, ref r) => write!(fmt, "({} OR {})", l, r),

            Expr::Json(s) => write!(fmt, "({})", s),

            Expr::Contains(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Contains, r),
            Expr::Eq(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Eq, r),
            Expr::Gt(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Gt, r),
            Expr::Lt(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Lt, r),
            Expr::Gte(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Gte, r),
            Expr::Lte(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Lte, r),
            Expr::Ne(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Ne, r),
            Expr::DoesNotContain(ref l, ref r) => {
                write!(fmt, "{}{}{}", l, ComparisonOpcode::DoesNotContain, r)
            }
            Expr::Regex(ref l, ref r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Regex, r),
            Expr::MoreLikeThis(ref l, ref r) => {
                write!(fmt, "{}{}{}", l, ComparisonOpcode::MoreLikeThis, r)
            }
            Expr::FuzzyLikeThis(ref l, ref r) => {
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
