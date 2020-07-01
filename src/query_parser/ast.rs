use std::fmt::{Debug, Error, Formatter};

pub enum Expr<'input> {
    Boolean(bool),
    Null,
    Value(&'input str),
    ParsedArray(Vec<(Box<Expr<'input>>, Option<&'input str>)>),
    UnparsedArray(&'input str),
    Range(Box<Expr<'input>>, Box<Expr<'input>>),
    Op(Box<Expr<'input>>, Opcode, Box<Expr<'input>>),
    UnaryOp(Opcode, Box<Expr<'input>>),
    Cmp(&'input str, ComparisonOpcode, Box<Expr<'input>>),
}

impl<'input> ToString for Expr<'input> {
    fn to_string(&self) -> String {
        match self {
            Expr::Boolean(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            Expr::Null => "NULL".to_string(),
            Expr::Value(s) => {
                let mut s = s.to_string();
                s.retain(|c| c != '\\');
                s
            }
            Expr::ParsedArray(v) => {
                let mut s = String::new();
                s.push('[');
                for (i, (elem, _)) in v.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    s.push_str(&elem.to_string());
                }
                s.push(']');
                s
            }
            Expr::Range(start, end) => format!("{} /TO/ {}", start.to_string(), end.to_string()),
            Expr::UnparsedArray(s) => s.to_string(),
            Expr::Op(_, _, _) => panic!("cannot convert Expr::Op to a String"),
            Expr::UnaryOp(_, _) => panic!("cannot convert Expr::UnaryOp to a String"),
            Expr::Cmp(_, _, _) => panic!("cannot convert Expr::Cmp to a String"),
        }
    }
}

#[derive(Copy, Clone)]
pub enum Opcode {
    Not,
    With,
    AndNot,
    And,
    Or,
}

#[derive(Copy, Clone)]
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

impl<'input> Debug for Expr<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        use self::Expr::*;
        match self {
            Boolean(_) => write!(fmt, "{}", self.to_string()),
            Null => write!(fmt, "{}", self.to_string()),
            Value(_) => write!(fmt, "'{}'", self.to_string().replace('\'', "\\'")),
            ParsedArray(_) => write!(fmt, "{}", self.to_string()),
            UnparsedArray(s) => write!(fmt, "{}", s),
            Range(_, _) => write!(fmt, "{}", self.to_string()),
            Op(ref l, op, ref r) => write!(fmt, "({:?} {:?} {:?})", l, op, r),
            UnaryOp(op, ref r) => write!(fmt, "({:?} ({:?}))", op, r),
            Cmp(fieldname, op, ref r) => write!(fmt, "{:}{:?}{:?}", fieldname, op, r),
        }
    }
}

impl Debug for Opcode {
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

impl Debug for ComparisonOpcode {
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
