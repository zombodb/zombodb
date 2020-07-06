use crate::highlighting::document_highlighter::*;
use crate::query_parser::ast::*;
use pgx::*;
use pgx::{JsonB, PgRelation};
use std::collections::HashMap;

struct QueryHighligther<'a> {
    document: serde_json::Value,
    query: Box<Expr<'a>>,
    highlighters: HashMap<&'a str, DocumentHighlighter>,
}

impl<'a> QueryHighligther<'a> {
    pub fn new(
        index: &PgRelation,
        document: serde_json::Value,
        fields: Vec<&'a str>,
        query: Box<Expr<'a>>,
    ) -> Self {
        let mut highlighters = HashMap::new();

        fields.into_iter().for_each(|field| {
            if let Some(value) = document
                .as_object()
                .expect("document not an object")
                .get(field)
            {
                // for now, assume field value is a string and that it exists
                let value = value.as_str().expect("field value not a string");

                let mut dh = DocumentHighlighter::new();
                info!("analyzing field={}, value={}", field, value);
                dh.analyze_document(index, field, value);

                highlighters.insert(field, dh);
            }
        });

        QueryHighligther {
            document,
            highlighters,
            query,
        }
    }

    pub fn highlight(&self) -> HashMap<QualifiedField, Vec<(String, TokenEntry)>> {
        let mut highlights = HashMap::new();
        self.walk_expression(&self.query, &mut highlights);
        highlights
    }

    fn walk_expression(
        &self,
        expr: &'a Box<Expr<'a>>,
        highlights: &mut HashMap<QualifiedField, Vec<(String, TokenEntry)>>,
    ) {
        match expr.as_ref() {
            Expr::Not(e) => self.walk_expression(e, highlights),
            Expr::With(l, r) => {
                self.walk_expression(l, highlights);
                self.walk_expression(r, highlights);
            }
            Expr::And(l, r) => {
                self.walk_expression(l, highlights);
                self.walk_expression(r, highlights);
            }
            Expr::Or(l, r) => {
                self.walk_expression(l, highlights);
                self.walk_expression(r, highlights);
            }

            Expr::Subselect(_, _) => panic!("subselect not supported yet"),
            Expr::Expand(_, _) => panic!("expand not supported yet"),
            Expr::Json(_) => panic!("json not supported yet"),

            Expr::Contains(f, t) | Expr::Eq(f, t) => {
                if let Some(dh) = self.highlighters.get(&f.field.as_str()) {
                    self.highlight_term(dh, f.clone(), t, highlights);
                }
            }

            Expr::Regex(_, _) => unimplemented!(),

            Expr::DoesNotContain(_, _) | Expr::Ne(_, _) => {}

            Expr::Gt(_, _) => unimplemented!(),
            Expr::Lt(_, _) => unimplemented!(),
            Expr::Gte(_, _) => unimplemented!(),
            Expr::Lte(_, _) => unimplemented!(),
            Expr::MoreLikeThis(_, _) => unimplemented!(),
            Expr::FuzzyLikeThis(_, _) => unimplemented!(),
        }
    }

    fn highlight_term(
        &self,
        highlighter: &'a DocumentHighlighter,
        field: QualifiedField,
        term: &Term,
        highlights: &mut HashMap<QualifiedField, Vec<(String, TokenEntry)>>,
    ) {
        let process_entries =
            |field,
             term,
             entries: Vec<(String, &TokenEntry)>,
             highlights: &mut HashMap<QualifiedField, Vec<(String, TokenEntry)>>| {
                // convert entries into owned TokenEntry values
                let mut entries = entries
                    .into_iter()
                    .map(|(term, entry)| (term, entry.clone()))
                    .collect();

                highlights
                    // fir this field in our map of highlights
                    .entry(field)
                    // add to existing entries
                    .and_modify(|v| v.append(&mut entries))
                    // or insert brand new entries
                    .or_insert(entries);
            };

        match term {
            Term::String(s, _) => {
                if let Some(mut entries) = highlighter.highlight_token(s) {
                    process_entries(field, term, entries, highlights);
                }
            }
            Term::Wildcard(s, _) => {
                if let Some(mut entries) = highlighter.highlight_wildcard(s) {
                    process_entries(field, term, entries, highlights);
                }
            }
            Term::Fuzzy(s, d, _) => {}
            Term::ProximityChain(v) => {}

            Term::ParsedArray(_, _) => {}
            Term::UnparsedArray(_, _) => {}
            Term::Range(_, _, _) => {}
            Term::Null => {}
        }
    }
}

#[pg_extern]
fn highlight_document(
    index: PgRelation,
    document: JsonB,
    query_string: &'static str,
) -> impl std::iter::Iterator<
    Item = (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    // select * from zdb.highlight_document('idxbeer', '{"subject":"free beer", "authoremail":"Christi l nicolay"}', '!!subject:beer or subject:fr?? and authoremail:(christi, nicolay)') order by field_name, position;
    let fields = vec!["subject", "authoremail"];
    let query = Expr::from_str(
        QualifiedIndex {
            schema: Some(index.namespace().to_string()),
            table: index
                .heap_relation()
                .expect("specified relation is not an index")
                .name()
                .to_string(),
            index: index.name().to_string(),
        },
        "_zdb_all",
        query_string,
    )
    .expect("failed to parse query");

    let mut qh = QueryHighligther::new(&index, document.0, fields, query);
    let highlights = qh.highlight();

    highlights
        .into_iter()
        .map(|(k, v)| {
            v.into_iter()
                .map(|(term, entry)| {
                    (
                        k.field.clone(),
                        term.clone(),
                        entry.type_.clone(),
                        entry.position as i32,
                        entry.start_offset as i64,
                        entry.end_offset as i64,
                    )
                })
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect::<Vec<_>>()
        .into_iter()
}
