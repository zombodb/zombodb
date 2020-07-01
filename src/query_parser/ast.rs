use std::fmt::{Debug, Display, Error, Formatter};

#[derive(Debug, Clone)]
pub struct ProximityPart<'input> {
    pub word: &'input str,
    pub distance: u32,
    pub in_order: bool,
}

#[derive(Debug, Clone)]
pub enum Opcode {
    Not,
    With,
    AndNot,
    And,
    Or,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum Expr<'input> {
    Null,
    String(&'input str),
    ParsedArray(Vec<(&'input str, Option<&'input str>)>),
    UnparsedArray(&'input str),
    Range(&'input str, &'input str),
    ProximityChain(Vec<ProximityPart<'input>>),
    Not(Box<Expr<'input>>),
    With(Box<Expr<'input>>, Box<Expr<'input>>),
    And(Box<Expr<'input>>, Box<Expr<'input>>),
    Or(Box<Expr<'input>>, Box<Expr<'input>>),
    Cmp(&'input str, ComparisonOpcode, Box<Expr<'input>>),
}

impl<'input> Display for Expr<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            Expr::Null => write!(fmt, "NULL"),
            Expr::String(s) => write!(fmt, "{}", s),
            Expr::ParsedArray(v) => {
                write!(fmt, "[")?;
                for (i, (elem, _)) in v.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, ",")?;
                    }
                    write!(fmt, "\"")?;
                    write!(fmt, "{}", elem.replace('"', "\\\""))?;
                    write!(fmt, "\"")?;
                }
                write!(fmt, "]")
            }
            Expr::UnparsedArray(s) => write!(fmt, "{}", s),
            Expr::Range(start, end) => write!(
                fmt,
                "\"{}\" /TO/ \"{}\"",
                start.replace('"', "\\\""),
                end.replace('"', "\\\"")
            ),
            Expr::ProximityChain(parts) => {
                write!(fmt, "(")?;
                let mut iter = parts.iter().peekable();
                while let Some(part) = iter.next() {
                    let next = iter.peek();

                    match next {
                        Some(_) => write!(
                            fmt,
                            "\"{}\" {}/{} ",
                            part.word.replace('"', "\\\""),
                            if part.in_order { "WO" } else { "W" },
                            part.distance
                        )?,
                        None => write!(fmt, "\"{}\"", part.word.replace('"', "\\\""))?,
                    };
                }
                write!(fmt, ")")
            }
            Expr::Not(ref r) => write!(fmt, "NOT ({})", r),
            Expr::With(ref l, ref r) => write!(fmt, "({} WITH {})", l, r),
            Expr::And(ref l, ref r) => write!(fmt, "({} AND {})", l, r),
            Expr::Or(ref l, ref r) => write!(fmt, "({} OR {})", l, r),
            Expr::Cmp(fieldname, op, ref r) => write!(fmt, "{}{}{}", fieldname, op, r),
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
            Ne => write!(fmt, "<>"),
            DoesNotContain => write!(fmt, "!="),
            Regex => write!(fmt, ":~"),
            MoreLikeThis => write!(fmt, ":@"),
            FuzzyLikeThis => write!(fmt, ":@~"),
        }
    }
}
