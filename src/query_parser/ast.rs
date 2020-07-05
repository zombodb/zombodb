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
    pub words: Vec<Expr<'input>>,
    pub distance: Option<ProximityDistance>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QualifiedIndex<'input>(pub Option<&'input str>, pub &'input str, pub &'input str);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexLink<'input> {
    pub name: Option<&'input str>,
    pub left_field: &'input str,
    pub qualified_index: QualifiedIndex<'input>,
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
pub enum Expr<'input> {
    // types of values
    Null,
    Json(String),
    String(&'input str, Option<f32>),
    Wildcard(&'input str, Option<f32>),
    Fuzzy(&'input str, u8, Option<f32>),
    ParsedArray(Vec<&'input str>, Option<f32>),
    UnparsedArray(&'input str, Option<f32>),
    Range(&'input str, &'input str, Option<f32>),

    ProximityChain(Vec<ProximityPart<'input>>),

    Subselect(IndexLink<'input>, Box<Expr<'input>>),
    Expand(IndexLink<'input>, Box<Expr<'input>>),

    // types of connectors
    Not(Box<Expr<'input>>),
    With(Box<Expr<'input>>, Box<Expr<'input>>),
    And(Box<Expr<'input>>, Box<Expr<'input>>),
    Or(Box<Expr<'input>>, Box<Expr<'input>>),

    // types of comparisons
    Contains(&'input str, Box<Expr<'input>>),
    Eq(&'input str, Box<Expr<'input>>),
    Gt(&'input str, Box<Expr<'input>>),
    Lt(&'input str, Box<Expr<'input>>),
    Gte(&'input str, Box<Expr<'input>>),
    Lte(&'input str, Box<Expr<'input>>),
    Ne(&'input str, Box<Expr<'input>>),
    DoesNotContain(&'input str, Box<Expr<'input>>),
    Regex(&'input str, Box<Expr<'input>>),
    MoreLikeThis(&'input str, Box<Expr<'input>>),
    FuzzyLikeThis(&'input str, Box<Expr<'input>>),
}

pub type ParserError<'input> = ParseError<usize, Token<'input>, &'static str>;

impl<'input> Expr<'input> {
    pub fn from_str(
        default_fieldname: &'input str,
        input: &'input str,
    ) -> Result<Box<Expr<'input>>, ParserError<'input>> {
        let parser = crate::query_parser::parser::ExprParser::new();
        let mut fieldname_stack = vec![default_fieldname];
        let mut operator_stack = vec![ComparisonOpcode::Contains];
        parser.parse(&mut fieldname_stack, &mut operator_stack, input)
    }

    pub(in crate::query_parser) fn from_opcode(
        field_name: &'input str,
        opcode: ComparisonOpcode,
        right: Box<Expr<'input>>,
    ) -> Expr<'input> {
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

    pub(in crate::query_parser) fn maybe_make_wildcard(expr: Expr<'input>) -> Expr<'input> {
        match expr {
            Expr::String(s, b) => {
                let mut prev = 0 as char;
                for c in s.chars() {
                    if (c == '*' || c == '?') && prev != '\\' {
                        return Expr::Wildcard(s, b);
                    }
                    prev = c;
                }
                expr
            }
            _ => expr,
        }
    }

    pub(in crate::query_parser) fn extract_prox_terms(&self) -> Vec<Expr<'input>> {
        let mut flat = Vec::new();
        match self {
            Expr::Or(l, r) => {
                flat.append(&mut l.extract_prox_terms());
                flat.append(&mut r.extract_prox_terms());
            }
            Expr::Contains(_, v) | Expr::Eq(_, v) | Expr::DoesNotContain(_, v) | Expr::Ne(_, v) => {
                flat.append(&mut v.extract_prox_terms());
            }
            Expr::String(s, b) => {
                flat.push(Expr::String(s, *b));
            }
            Expr::Wildcard(s, b) => {
                flat.push(Expr::Wildcard(s, *b));
            }
            Expr::Fuzzy(s, d, b) => {
                flat.push(Expr::Fuzzy(s, *d, *b));
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

impl<'input> Display for QualifiedIndex<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        if let Some(schema) = self.0 {
            write!(fmt, "{}.{}.{}", schema, self.1, self.2)
        } else {
            write!(fmt, "{}.{}", self.1, self.2)
        }
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

impl<'input> Display for Expr<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            Expr::Null => write!(fmt, "NULL"),

            Expr::Json(s) => write!(fmt, "{}", s),

            Expr::String(s, b) | Expr::Wildcard(s, b) => {
                write!(fmt, "\"{}\"", s.replace('"', "\\\""))?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Expr::Fuzzy(s, f, b) => {
                write!(fmt, "\"{}\"~{}", s.replace('"', "\\\""), f)?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Expr::ParsedArray(a, b) => {
                write!(fmt, "[")?;
                for (i, elem) in a.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, ",")?;
                    }
                    write!(fmt, "\"")?;
                    write!(fmt, "{}", elem.replace('"', "\\\""))?;
                    write!(fmt, "\"")?;
                }
                write!(fmt, "]")?;

                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Expr::UnparsedArray(a, b) => {
                write!(fmt, "[[{}]]", a)?;
                if let Some(boost) = b {
                    write!(fmt, "^{}", boost)?;
                }
                Ok(())
            }

            Expr::Range(start, end, b) => {
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

            Expr::ProximityChain(parts) => {
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

            Expr::Subselect(ref link, ref q) => write!(fmt, "#subselect<{}>({})", link, q),
            Expr::Expand(ref link, ref q) => write!(fmt, "#expand<{}>({})", link, q),

            Expr::Not(ref r) => write!(fmt, "NOT ({})", r),
            Expr::With(ref l, ref r) => write!(fmt, "({} WITH {})", l, r),
            Expr::And(ref l, ref r) => write!(fmt, "({} AND {})", l, r),
            Expr::Or(ref l, ref r) => write!(fmt, "({} OR {})", l, r),

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
