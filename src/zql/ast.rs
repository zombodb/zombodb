use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Display, Error, Formatter};
use std::str::FromStr;

use lalrpop_util::ParseError;
use pgx::PgRelation;
use serde::{Deserialize, Serialize};

pub use pg_catalog::ProximityPart;
pub use pg_catalog::ProximityTerm;

use crate::access_method::options::ZDBIndexOptions;
use crate::utils::{find_zdb_index, get_null_copy_to_fields};
use crate::zql::parser::Token;
use crate::zql::relationship_manager::RelationshipManager;
use crate::zql::transformations::expand::expand;
use crate::zql::transformations::expand_index_links::expand_index_links;
use crate::zql::transformations::field_finder::{find_fields, find_link_for_field};
use crate::zql::transformations::field_lists::expand_field_lists;
use crate::zql::transformations::index_links::assign_links;
use crate::zql::transformations::nested_groups::group_nested;
use crate::zql::transformations::prox_rewriter::rewrite_proximity_chains;
use crate::zql::transformations::retarget::retarget_expr;
use crate::zql::{INDEX_LINK_PARSER, ZDB_QUERY_PARSER};

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProximityDistance {
    pub distance: u32,
    pub in_order: bool,
}

impl Default for ProximityDistance {
    fn default() -> Self {
        ProximityDistance {
            distance: 0,
            in_order: false,
        }
    }
}

#[pgx_macros::pg_schema]
pub mod pg_catalog {
    use pgx::*;
    use serde::{Deserialize, Serialize};

    use crate::zql::ast::ProximityDistance;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub enum ProximityTerm {
        String(String, Option<f32>),
        Phrase(String, Option<f32>),
        Prefix(String, Option<f32>),
        Wildcard(String, Option<f32>),
        Fuzzy(String, u8, Option<f32>),
        Regex(String, Option<f32>),
        ProximityChain(Vec<ProximityPart>),
    }

    #[derive(Debug, Clone, PartialEq, PostgresType, Serialize, Deserialize)]
    pub struct ProximityPart {
        pub words: Vec<ProximityTerm>,
        pub distance: Option<ProximityDistance>,
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct QualifiedIndex {
    pub schema: Option<String>,
    pub table: String,
    pub index: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct QualifiedField {
    pub index: Option<IndexLink>,
    pub field: String,
}

#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
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
    Matches,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term<'input> {
    Null,
    MatchAll,
    String(&'input str, Option<f32>),
    Phrase(&'input str, Option<f32>),
    Prefix(&'input str, Option<f32>),
    PhrasePrefix(&'input str, Option<f32>),
    PhraseWithWildcard(&'input str, Option<f32>),
    Wildcard(&'input str, Option<f32>),
    Regex(&'input str, Option<f32>),
    Fuzzy(&'input str, u8, Option<f32>),
    Range(&'input str, &'input str, Option<f32>),
    ParsedArray(Vec<Term<'input>>, Option<f32>),
    UnparsedArray(&'input str, Option<f32>),

    ProximityChain(Vec<ProximityPart>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'input> {
    Null,

    Subselect(IndexLink, Box<Expr<'input>>),
    Expand(IndexLink, Box<Expr<'input>>, Option<Box<Expr<'input>>>),

    // types of connectors
    Not(Box<Expr<'input>>),

    WithList(Vec<Expr<'input>>),
    AndList(Vec<Expr<'input>>),
    OrList(Vec<Expr<'input>>),

    Linked(IndexLink, Box<Expr<'input>>),
    Nested(String, Box<Expr<'input>>),

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
    Matches(QualifiedField, Term<'input>),
}

impl<'input> Term<'input> {
    pub(in crate::zql) fn maybe_make_wildcard_or_regex(
        opcode: Option<&ComparisonOpcode>,
        s: &'input str,
        b: Option<f32>,
    ) -> Term<'input> {
        if let Some(&ComparisonOpcode::Regex) = opcode {
            Term::Regex(s, b)
        } else if s.len() == 1 && s.chars().nth(0) == Some('*') {
            Term::MatchAll
        } else {
            let mut has_whitespace = false;
            let mut is_wildcard = false;
            let mut is_fuzzy = false;
            let mut prev = 0 as char;
            for c in s.chars() {
                if prev != '\\' {
                    if c == '*' || c == '?' {
                        is_wildcard = true;
                    } else if c == '~' {
                        is_fuzzy = true;
                    } else if c.is_whitespace() {
                        has_whitespace = true;
                    }
                }

                prev = c;
            }

            if has_whitespace && (is_wildcard || is_fuzzy) {
                if Term::is_prefix_wildcard(s) {
                    Term::PhrasePrefix(s, b)
                } else {
                    Term::PhraseWithWildcard(s, b)
                }
            } else if is_wildcard {
                if Term::is_prefix_wildcard(s) {
                    Term::Prefix(s, b)
                } else if Term::is_all_asterisks(s) {
                    Term::Wildcard("*", b)
                } else {
                    Term::Wildcard(s, b)
                }
            } else if has_whitespace {
                Term::Phrase(s, b)
            } else if Term::might_be_a_phrase(s) {
                Term::Phrase(s, b)
            } else {
                Term::String(s, b)
            }
        }
    }

    pub fn is_prefix_wildcard(s: &str) -> bool {
        s.chars().last() == Some('*')
            && s.chars().filter(|c| *c == '*').count() == 1
            && s.chars().filter(|c| *c == '?').count() == 0
    }

    pub fn is_all_asterisks(s: &str) -> bool {
        for c in s.chars() {
            if c != '*' {
                return false;
            }
        }
        true
    }

    fn might_be_a_phrase(input: &str) -> bool {
        input.chars().any(|c| !c.is_alphanumeric() && c != '_')
    }
}

impl ProximityTerm {
    pub fn from_term(term: &Term) -> Self {
        match term {
            Term::String(s, b) => ProximityTerm::String(s.to_string(), *b),
            Term::Phrase(s, b) => ProximityTerm::Phrase(s.to_string(), *b),
            Term::Prefix(s, b) => ProximityTerm::Prefix(s.to_string(), *b),
            Term::PhrasePrefix(s, b) => ProximityTerm::Phrase(s.to_string(), *b),
            Term::ProximityChain(v) => ProximityTerm::ProximityChain(v.clone()),
            Term::Wildcard(s, b) => ProximityTerm::Wildcard(s.to_string(), *b),
            Term::Regex(s, b) => ProximityTerm::Regex(s.to_string(), *b),
            Term::Fuzzy(s, d, b) => ProximityTerm::Fuzzy(s.to_string(), *d, *b),
            Term::MatchAll => ProximityTerm::Wildcard("*".to_string(), None),
            _ => panic!("Cannot convert {:?} into a ProximityTerm", term),
        }
    }

    pub fn to_term(&self) -> Term {
        // NB:  we don't use the boosts here when we convert to a Term::xxx b/c ES doesn't support
        // them in span_near clauses.  But ProximityTerm holds onto the boost anyways for potential
        // future proofing
        match self {
            ProximityTerm::String(s, _b) => Term::String(&s, None),
            ProximityTerm::Phrase(s, _b) => Term::Phrase(&s, None),
            ProximityTerm::Prefix(s, _b) => Term::Prefix(&s, None),
            ProximityTerm::Wildcard(w, _b) => {
                if w == "*" {
                    Term::MatchAll
                } else {
                    Term::Wildcard(&w, None)
                }
            }
            ProximityTerm::Fuzzy(f, d, _b) => Term::Fuzzy(&f, *d, None),
            ProximityTerm::Regex(r, _b) => Term::Regex(&r, None),
            ProximityTerm::ProximityChain(p) => Term::ProximityChain(p.clone()),
        }
    }

    pub fn to_terms(terms: &Vec<ProximityTerm>) -> Vec<Term> {
        terms.iter().map(|v| v.to_term()).collect()
    }

    pub fn make_proximity_term(
        opcode: Option<&ComparisonOpcode>,
        s: &str,
        b: Option<f32>,
    ) -> ProximityTerm {
        let term = Term::maybe_make_wildcard_or_regex(opcode, s, b);
        ProximityTerm::from_term(&term)
    }

    pub fn make_proximity_chain(
        field: &QualifiedField,
        input: &str,
        b: Option<f32>,
    ) -> ProximityTerm {
        let replaced_input = input.replace("*", "ZDBSTAR");
        let replaced_input = replaced_input.replace("?", "ZDBQUESTION");

        let index = field.index.as_ref();

        let analyzed_tokens: Vec<_> = match index {
            // if we have an index we'll ask Elasticsearch to tokenize the input for us
            #[cfg(not(feature = "pg_test"))]
            Some(index) => {
                let index = index
                    .open_index()
                    .expect(&format!("failed to open index for {}", field));
                let analyzer = crate::utils::get_search_analyzer(&index, &field.field);
                crate::elasticsearch::Elasticsearch::new(&index)
                    .analyze_text(&analyzer, &replaced_input)
                    .execute()
                    .expect("failed to analyze phrase")
                    .tokens
            }

            // if we don't then we'll just tokenize on whitespace.  This is only going to happen
            // during disconnected tests.  Generally, this means only during tests
            _ => {
                fn split_whitespace_indices(s: &str) -> impl Iterator<Item = (usize, &str)> {
                    #[inline(always)]
                    fn addr_of(s: &str) -> usize {
                        s.as_ptr() as usize
                    }

                    s.split_whitespace()
                        .map(move |sub| (addr_of(sub) - addr_of(s), sub))
                }
                split_whitespace_indices(&replaced_input)
                    .enumerate()
                    .map(
                        |(idx, (start_offset, word))| crate::elasticsearch::analyze::Token {
                            type_: "".to_string(),
                            token: word.to_string(),
                            position: idx as i32,
                            start_offset: start_offset as i64,
                            end_offset: (start_offset + word.len()) as i64,
                        },
                    )
                    .collect()
            }
        };

        // first, group tokens by end_offset
        let mut groups = BTreeMap::<i64, Vec<crate::elasticsearch::analyze::Token>>::new();
        for token in analyzed_tokens {
            groups.entry(token.end_offset).or_default().push(token);
        }

        if groups.len() == 0 {
            // input did not analyze into any tokens, so we convert it to lowercase and
            // make it into the type of ProximityTerm it might be (string, wildcard, regex, fuzzy)
            let lowercase_input = input.to_lowercase();
            let term = Term::maybe_make_wildcard_or_regex(None, &lowercase_input, b);
            ProximityTerm::from_term(&term)
        } else if groups.len() == 1 {
            // input analyzed into 1 token, so we make it into the type of ProximityTerm it
            // might be (string, wildcard, regex, fuzzy)
            let token = groups.values().into_iter().next().unwrap().get(0).unwrap();
            let token = ProximityTerm::replace_substitutions(token);
            let term = Term::maybe_make_wildcard_or_regex(None, &token, b);
            ProximityTerm::from_term(&term)
        } else {
            // next, build ProximityParts for each group
            let proximity_parts: Vec<ProximityPart> = groups
                .into_iter()
                .map(|(_, tokens)| {
                    let mut terms = Vec::new();

                    for token in tokens {
                        let token = ProximityTerm::replace_substitutions(&token);
                        let term = Term::maybe_make_wildcard_or_regex(None, &token, b);
                        terms.push(ProximityTerm::from_term(&term));
                    }

                    ProximityPart {
                        words: terms,
                        distance: Some(ProximityDistance {
                            distance: 0,
                            in_order: true,
                        }),
                    }
                })
                .collect();

            let mut filtered_parts = Vec::with_capacity(proximity_parts.len());
            let mut iter = proximity_parts.into_iter().peekable();
            while let Some(mut part) = iter.next() {
                let mut next = iter.peek();

                // look for bare '*' ProximityTerms and just skip them entirely, incrementing
                // the current part's distance for each consecutive '*' we find
                while next.is_some() {
                    let current_distance = part.distance.as_mut().unwrap().distance;
                    pgx::check_for_interrupts!();

                    let unwrapped = next.unwrap();
                    if unwrapped.words.len() == 1 {
                        if let ProximityTerm::Wildcard(s, _) = unwrapped.words.get(0).unwrap() {
                            if s == "*" {
                                part.distance.as_mut().unwrap().distance += 1;
                                // just consume the next token as we want to skip it entirely
                                iter.next();
                                next = iter.peek();
                            }
                        }
                    }

                    if part.distance.as_mut().unwrap().distance == current_distance {
                        // we didn't make a change so we're done looking for bare '*'
                        break;
                    }
                }

                filtered_parts.push(part);
            }

            ProximityTerm::ProximityChain(filtered_parts)
        }
    }

    fn replace_substitutions(token: &crate::elasticsearch::analyze::Token) -> String {
        let token = token.token.replace("ZDBSTAR", "*");
        let token = token.replace("ZDBQUESTION", "?");
        let token = token.replace("zdbstar", "*");
        let token = token.replace("zdbquestion", "?");
        token
    }
}

pub type ParserError<'input> = ParseError<usize, Token<'input>, &'static str>;

impl<'input> Expr<'input> {
    pub fn from_str(
        index: &PgRelation,
        default_fieldname: &'input str,
        input: &'input str,
        index_links: &Vec<IndexLink>,
        target_link: &Option<IndexLink>,
        used_fields: &mut HashSet<&'input str>,
    ) -> Result<Expr<'input>, ParserError<'input>> {
        if input.trim().is_empty() {
            // empty strings just match everything
            return Ok(Expr::Json("{ match_all: {} }".into()));
        }

        let root_index = IndexLink::from_relation(index);
        let zdboptions = ZDBIndexOptions::from_relation(index);

        Expr::from_str_disconnected(
            Some(index),
            default_fieldname,
            input,
            used_fields,
            root_index,
            index_links,
            target_link,
            zdboptions.field_lists(),
        )
    }

    pub fn from_str_disconnected(
        index: Option<&PgRelation>,
        default_fieldname: &'input str,
        input: &'input str,
        used_fields: &mut HashSet<&'input str>,
        root_index: IndexLink,
        index_links: &Vec<IndexLink>,
        target_link: &Option<IndexLink>,
        mut field_lists: HashMap<String, Vec<QualifiedField>>,
    ) -> Result<Expr<'input>, ParserError<'input>> {
        let input = input.clone();
        let mut operator_stack = vec![ComparisonOpcode::Contains];
        let mut fieldname_stack = vec![default_fieldname];

        let mut expr = *ZDB_QUERY_PARSER.with(|parser| {
            parser.parse(
                index,
                used_fields,
                &mut fieldname_stack,
                &mut operator_stack,
                input,
            )
        })?;

        let used_zdb_all = used_fields.contains("zdb_all");
        if used_zdb_all || !field_lists.is_empty() {
            if used_zdb_all && index.is_some() {
                // create a field list for "zdb_all"
                for (link, relation) in index
                    .into_iter()
                    .map(|index| (&root_index, index.clone()))
                    .chain(
                        index_links
                            .iter()
                            .map(|link| (link, link.open_index().expect("failed to open index"))),
                    )
                {
                    let fields = get_null_copy_to_fields(&relation);
                    field_lists.entry("zdb_all".into()).or_default().append(
                        &mut fields
                            .into_iter()
                            .map(|field| QualifiedField {
                                index: Some(link.clone()),
                                field,
                            })
                            .collect(),
                    );
                }
            }

            expand_field_lists(&mut expr, &field_lists);
        }

        expand(&mut expr, &root_index, index_links);
        find_fields(&mut expr, &root_index, index_links);
        group_nested(&index, &mut expr);

        let mut relationship_manager = RelationshipManager::new();
        for link in index_links {
            let mut left_field = link.left_field.as_ref().unwrap().as_str();
            if left_field.contains('.') {
                let mut parts = left_field.split('.');
                parts.next();
                left_field = parts.next().unwrap();
            }
            let left_link =
                find_link_for_field(&link.qualify_left_field(), &root_index, &index_links)
                    .expect("unable to find link for field");
            relationship_manager.add_relationship(
                &left_link,
                left_field,
                link,
                link.right_field.as_ref().unwrap(),
            )
        }

        assign_links(&root_index, &mut expr, index_links);
        expand_index_links(&mut expr, &root_index, &mut relationship_manager);
        rewrite_proximity_chains(&mut expr);
        expr = retarget_expr(expr, &root_index, target_link, &mut relationship_manager);
        Ok(expr)
    }

    pub(in crate::zql) fn from_opcode(
        field: &'input str,
        opcode: ComparisonOpcode,
        right: Term<'input>,
    ) -> Expr<'input> {
        let field_name = QualifiedField {
            index: None,
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
            ComparisonOpcode::Matches => Expr::Matches(field_name, right),
        }
    }

    pub(in crate::zql) fn range_from_opcode(
        field: &'input str,
        opcode: ComparisonOpcode,
        start: (&'input str, bool),
        end: (&'input str, bool),
        boost: Option<f32>,
    ) -> Expr<'input> {
        let start = start.0;
        let end = end.0;
        let field_name = QualifiedField {
            index: None,
            field: field.to_string(),
        };

        let range = Term::Range(start, end, boost);
        match opcode {
            ComparisonOpcode::Contains => Expr::Contains(field_name, range),
            ComparisonOpcode::Eq => Expr::Eq(field_name, range),
            ComparisonOpcode::Ne => Expr::Ne(field_name, range),
            ComparisonOpcode::DoesNotContain => Expr::DoesNotContain(field_name, range),
            _ => panic!("invalid operator for range query"),
        }
    }

    pub(in crate::zql) fn extract_prox_terms(
        &self,
        index: Option<&PgRelation>,
    ) -> Vec<ProximityTerm> {
        let mut flat = Vec::new();
        match self {
            Expr::OrList(v) => {
                v.iter()
                    .for_each(|e| flat.append(&mut e.extract_prox_terms(index)));
            }
            Expr::Contains(_, term)
            | Expr::Eq(_, term)
            | Expr::DoesNotContain(_, term)
            | Expr::Ne(_, term) => {
                flat.push(ProximityTerm::from_term(term));
            }
            _ => panic!("Unsupported proximity group expression: {}", self),
        }
        flat
    }

    pub fn nested_path(exprs: &Vec<Expr<'input>>) -> Option<String> {
        let mut path = None;

        for e in exprs {
            let local_path = e.get_nested_path();

            // get the top-level path, if we have one
            let local_path = if local_path.is_some() {
                Some(local_path.unwrap().split('.').next().unwrap().to_string())
            } else {
                local_path
            };

            if path.is_none() {
                path = local_path;
            } else if path != local_path {
                return None;
            }
        }

        path
    }

    pub fn get_nested_path(&self) -> Option<String> {
        match self {
            Expr::Null => unreachable!(),
            Expr::Subselect(_, _) => panic!("#subselect not supported in WITH clauses"),
            Expr::Expand(_, _, _) => panic!("#expand not supported in WITH clauses"),
            Expr::Not(e) => e.get_nested_path(),
            Expr::WithList(v) => Expr::nested_path(v),
            Expr::AndList(v) => Expr::nested_path(v),
            Expr::OrList(v) => Expr::nested_path(v),
            Expr::Linked(_, e) => e.get_nested_path(),
            Expr::Nested(p, _) => Some(p.to_string()),
            Expr::Json(_) => panic!("json not supported in WITH clauses"),
            Expr::Contains(f, _) => f.nested_path(),
            Expr::Eq(f, _) => f.nested_path(),
            Expr::Gt(f, _) => f.nested_path(),
            Expr::Lt(f, _) => f.nested_path(),
            Expr::Gte(f, _) => f.nested_path(),
            Expr::Lte(f, _) => f.nested_path(),
            Expr::Ne(f, _) => f.nested_path(),
            Expr::DoesNotContain(f, _) => f.nested_path(),
            Expr::Regex(f, _) => f.nested_path(),
            Expr::MoreLikeThis(f, _) => f.nested_path(),
            Expr::FuzzyLikeThis(f, _) => f.nested_path(),
            Expr::Matches(f, _) => f.nested_path(),
        }
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

impl FromStr for QualifiedIndex {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('.').map(|p| p.to_string());
        let schema = parts.next();
        let table = parts.next();
        let index = parts.next();

        if table.is_none() || index.is_none() {
            Err("QualifiedIndex string not in proper format")
        } else {
            Ok(QualifiedIndex {
                schema,
                table: table.unwrap(),
                index: index.unwrap(),
            })
        }
    }
}

impl QualifiedIndex {
    pub fn from_relation(index: &PgRelation) -> Self {
        let (index, _) = find_zdb_index(index).unwrap();
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

    pub fn index_name(&self) -> String {
        let mut relation_name = String::new();
        if let Some(schema) = &self.schema {
            relation_name.push_str(&schema);
            relation_name.push('.');
        }
        relation_name.push_str(&self.index);
        relation_name
    }

    pub fn qualified_name(&self) -> String {
        let mut relation_name = self.table_name();
        relation_name.push('.');
        relation_name.push_str(&self.index);
        relation_name
    }
}

impl IndexLink {
    pub fn parse(input: &str) -> Self {
        INDEX_LINK_PARSER.with(|parser| {
            parser
                .parse(
                    None,
                    &mut HashSet::new(),
                    &mut Vec::new(),
                    &mut Vec::new(),
                    input,
                )
                .expect("failed to parse IndexLink")
        })
    }

    pub fn from_relation(index: &PgRelation) -> Self {
        IndexLink {
            name: None,
            left_field: None,
            qualified_index: QualifiedIndex::from_relation(index),
            right_field: None,
        }
    }

    pub fn from_options(index: &PgRelation, options: Option<Vec<String>>) -> Vec<Self> {
        match options {
            None => Vec::new(),
            Some(options) => {
                let mut index_links = Vec::new();

                for link in options {
                    let link = INDEX_LINK_PARSER.with(|parser| {
                        parser
                            .parse(
                                Some(index),
                                &mut HashSet::new(),
                                &mut Vec::new(),
                                &mut Vec::new(),
                                link.as_str(),
                            )
                            .expect("failed to parse index link")
                    });
                    index_links.push(link);
                }

                index_links
            }
        }
    }

    pub fn from_zdb(index: &PgRelation) -> Vec<Self> {
        let mut index_links = Vec::new();

        if let Some(links) = ZDBIndexOptions::from_relation(&index).links() {
            for link in links {
                let mut used_fields = HashSet::new();
                let mut fieldname_stack = Vec::new();
                let mut operator_stack = Vec::new();
                let link = INDEX_LINK_PARSER.with(|parser| {
                    parser
                        .parse(
                            Some(&index),
                            &mut used_fields,
                            &mut fieldname_stack,
                            &mut operator_stack,
                            link.as_str(),
                        )
                        .expect("failed to parse index link")
                });
                index_links.push(link);
            }
        }

        index_links
    }

    pub fn is_this_index(&self) -> bool {
        (self.qualified_index.schema.is_none()
            || self.qualified_index.schema == Some("public".into()))
            && self.qualified_index.table == "this"
            && self.qualified_index.index == "index"
    }

    pub fn open_table(&self) -> std::result::Result<PgRelation, &str> {
        PgRelation::open_with_name_and_share_lock(&self.qualified_index.table_name())
    }

    pub fn open_index(&self) -> std::result::Result<PgRelation, &str> {
        PgRelation::open_with_name_and_share_lock(&self.qualified_index.index_name())
    }

    pub fn qualify_left_field(&self) -> QualifiedField {
        QualifiedField {
            index: None,
            field: self.left_field.as_ref().unwrap().clone(),
        }
    }
}

impl QualifiedField {
    pub fn nested_path(&self) -> Option<String> {
        if self.field.contains('.') {
            let mut parts = self.field.rsplitn(2, '.');
            let _ = parts.next();
            let second = parts.next();

            second.map(|v| v.to_string())
        } else {
            None
        }
    }

    pub fn base_field(&self) -> String {
        self.field.split('.').next().map(|v| v.to_string()).unwrap()
    }

    pub fn field_name(&self) -> String {
        if let Some(index) = self.index.as_ref() {
            if index.name == Some(self.base_field().to_string())
                || &index.qualified_index.table == &self.base_field()
            {
                return self.field.splitn(2, '.').last().unwrap().to_string();
            }
        }

        return self.field.clone();
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

impl Debug for IndexLink {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "IndexLink({})", self)
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

impl Display for ProximityTerm {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), Error> {
        write!(fmt, "{}", self.to_term())
    }
}

impl<'input> Display for Term<'input> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            Term::Null => write!(fmt, "NULL"),

            Term::MatchAll => write!(fmt, "*"),

            Term::String(s, b)
            | Term::Phrase(s, b)
            | Term::PhraseWithWildcard(s, b)
            | Term::Prefix(s, b)
            | Term::PhrasePrefix(s, b)
            | Term::Wildcard(s, b)
            | Term::Regex(s, b) => {
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
            Expr::Null => unreachable!(),

            Expr::Subselect(link, q) => write!(fmt, "#subselect<{}>({})", link, q),
            Expr::Expand(link, q, f) => {
                write!(fmt, "#expand<{}>({}", link, q)?;
                if let Some(filter) = f {
                    write!(fmt, " #filter({})", filter)?;
                }
                write!(fmt, ")")
            }

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

            Expr::Nested(_, e) => write!(fmt, "{}", e),

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
            Expr::Matches(l, r) => write!(fmt, "{}{}{}", l, ComparisonOpcode::Matches, r),
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
            Matches => write!(fmt, "==>"),
        }
    }
}
