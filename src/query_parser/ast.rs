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
use crate::query_parser::transformations::field_finder::find_fields;
use crate::query_parser::transformations::index_links::assign_links;
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct QualifiedIndex {
    pub schema: Option<String>,
    pub table: String,
    pub index: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct QualifiedField<'input> {
    pub index: Option<IndexLink>,
    pub field: &'input str,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct IndexLink {
    pub name: Option<String>,
    pub left_field: Option<String>,
    pub qualified_index: QualifiedIndex,
    pub right_field: Option<String>,
}

impl Default for IndexLink {
    fn default() -> Self {
        IndexLink {
            name: None,
            left_field: None,
            qualified_index: QualifiedIndex {
                schema: None,
                table: "table".to_string(),
                index: "index".to_string(),
            },
            right_field: None,
        }
    }
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
    Subselect(IndexLink, Box<Expr<'input>>),
    Expand(IndexLink, Box<Expr<'input>>),

    // types of connectors
    Not(Box<Expr<'input>>),

    WithList(Vec<Expr<'input>>),
    AndList(Vec<Expr<'input>>),
    OrList(Vec<Expr<'input>>),

    Linked(IndexLink, Box<Expr<'input>>),

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
        index: &PgRelation,
        default_fieldname: &'input str,
        input: &'input str,
        used_fields: &mut HashSet<&'input str>,
    ) -> Result<Expr<'input>, ParserError<'input>> {
        let root_index = IndexLink {
            name: None,
            left_field: None,
            qualified_index: QualifiedIndex::from_relation(index),
            right_field: None,
        };

        Expr::from_str_disconnected(
            default_fieldname,
            input,
            used_fields,
            root_index,
            IndexLink::from_zdb(index),
        )
    }

    pub fn from_str_disconnected(
        default_fieldname: &'input str,
        input: &'input str,
        used_fields: &mut HashSet<&'input str>,
        root_index: IndexLink,
        linked_indexes: Vec<IndexLink>,
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

        find_fields(expr.as_mut(), &root_index, &linked_indexes);
        if let Some(final_link) = assign_links(&root_index, expr.as_mut(), &linked_indexes) {
            if final_link != root_index {
                // the final link isn't the same as the root_index, so it needs to be wrapped
                // in an Expr::Linked
                return Ok(Expr::Linked(final_link, expr));
            }
        }
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
            Expr::OrList(v) => {
                v.iter()
                    .for_each(|e| flat.append(&mut e.extract_prox_terms()));
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

    pub fn table_name(&self) -> String {
        let mut relation_name = String::new();
        if let Some(schema) = &self.schema {
            relation_name.push_str(&schema);
            relation_name.push('.');
        }
        relation_name.push_str(&self.table);
        relation_name
    }
}

impl IndexLink {
    #[cfg(any(test, feature = "pg_test"))]
    pub fn parse(input: &str) -> Self {
        let parser = IndexLinkParser::new();
        parser
            .parse(&mut HashSet::new(), &mut Vec::new(), &mut Vec::new(), input)
            .expect("failed to parse IndexLink")
    }

    pub fn from_zdb(index: &PgRelation) -> Vec<Self> {
        let mut index_links = Vec::new();

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
                index_links.push(link);
            }
        }

        index_links
    }

    pub fn is_this_index(&self) -> bool {
        self.qualified_index.schema.is_none()
            && self.qualified_index.table == "this"
            && self.qualified_index.index == "index"
    }

    pub fn open(&self) -> std::result::Result<PgRelation, &str> {
        PgRelation::open_with_name_and_share_lock(&self.qualified_index.table_name())
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

impl Display for IndexLink {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        if let Some(name) = &self.name {
            write!(fmt, "{}:(", name)?;
        }

        let left_field = self
            .left_field
            .as_ref()
            .cloned()
            .unwrap_or(String::from("NONE"));
        let right_field = self
            .right_field
            .as_ref()
            .cloned()
            .unwrap_or(String::from("NONE"));

        write!(
            fmt,
            "{}=<{}>{}",
            left_field, self.qualified_index, right_field
        )?;

        if let Some(_) = &self.name {
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

            Expr::WithList(v) => {
                write!(fmt, "(")?;
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " WITH ")?;
                    }
                    write!(fmt, "{}", e)?;
                }
                write!(fmt, ")")
            }

            Expr::AndList(v) => {
                write!(fmt, "(")?;
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " AND ")?;
                    }
                    write!(fmt, "{}", e)?;
                }
                write!(fmt, ")")
            }

            Expr::OrList(v) => {
                write!(fmt, "(")?;
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " OR ")?;
                    }
                    write!(fmt, "{}", e)?;
                }
                write!(fmt, ")")
            }

            Expr::Linked(_, e) => write!(fmt, "{{{}}}", e),

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
