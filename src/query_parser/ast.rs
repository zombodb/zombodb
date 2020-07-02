use std::fmt::{Debug, Display, Error, Formatter};

#[derive(Debug, Clone)]
pub struct ProximityPart<'input> {
    pub word: &'input str,
    pub distance: u32,
    pub in_order: bool,
}

#[derive(Debug, Clone)]
pub enum Opcode {
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
    // types of values
    Null,
    String(&'input str),
    ParsedArray(Vec<(&'input str, Option<&'input str>)>),
    UnparsedArray(&'input str),
    Range(&'input str, &'input str),
    ProximityChain(Vec<ProximityPart<'input>>),

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

impl<'input> Expr<'input> {
    pub fn from_opcode(
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
}

impl<'input> Display for Expr<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            Expr::Null => write!(fmt, "NULL"),
            Expr::String(s) => write!(fmt, "\"{}\"", s.replace('"', "\\\"")),
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
