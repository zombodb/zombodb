use crate::highlighting::document_highlighter::*;
use crate::query_parser::ast::*;
use pgx::*;
use pgx::{JsonB, PgRelation};
use std::collections::HashMap;

struct QueryHighligther<'a> {
    query: Box<Expr<'a>>,
    highlighters: HashMap<&'a str, DocumentHighlighter<'a>>,
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
            highlighters,
            query,
        }
    }

    pub fn highlight(&'a self) -> HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>> {
        let mut highlights = HashMap::new();
        self.walk_expression(&self.query, &mut highlights);
        highlights
    }

    fn walk_expression(
        &'a self,
        expr: &'a Box<Expr<'a>>,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) -> bool {
        match expr.as_ref() {
            Expr::Not(e) => {
                let mut tmp_highlights = HashMap::new();
                !self.walk_expression(e, &mut tmp_highlights)
            }

            Expr::With(l, r) | Expr::And(l, r) => {
                let mut did_highlight = false;
                let mut tmp_highlights = HashMap::new();

                did_highlight = self.walk_expression(l, &mut tmp_highlights);
                if did_highlight {
                    did_highlight &= self.walk_expression(r, &mut tmp_highlights);
                }

                if did_highlight {
                    tmp_highlights.into_iter().for_each(|(k, v)| {
                        highlights.insert(k, v);
                    });
                }
                did_highlight
            }

            Expr::Or(l, r) => {
                let mut did_highlight = false;
                did_highlight |= self.walk_expression(l, highlights);
                did_highlight |= self.walk_expression(r, highlights);
                did_highlight
            }

            Expr::Subselect(_, _) => panic!("subselect not supported yet"),
            Expr::Expand(_, _) => panic!("expand not supported yet"),
            Expr::Json(_) => panic!("json not supported yet"),

            Expr::Contains(f, t) | Expr::Eq(f, t) => {
                let mut did_highlight = false;
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    did_highlight = self.highlight_term(dh, f.clone(), expr, t, highlights);
                }
                did_highlight
            }

            Expr::Regex(_, _) => unimplemented!(),

            Expr::DoesNotContain(_, _) | Expr::Ne(_, _) => unimplemented!(),

            Expr::Gt(_, _) => unimplemented!(),
            Expr::Lt(_, _) => unimplemented!(),
            Expr::Gte(_, _) => unimplemented!(),
            Expr::Lte(_, _) => unimplemented!(),
            Expr::MoreLikeThis(_, _) => unimplemented!(),
            Expr::FuzzyLikeThis(_, _) => unimplemented!(),
        }
    }

    fn highlight_term(
        &'a self,
        highlighter: &'a DocumentHighlighter,
        field: QualifiedField,
        expr: &'a Box<Expr<'a>>,
        term: &Term,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) -> bool {
        let process_entries = |field,
                               mut entries: Vec<(String, &'a TokenEntry)>,
                               highlights: &mut HashMap<
            (QualifiedField, String),
            Vec<(String, &'a TokenEntry)>,
        >| {
            highlights
                // fir this field in our map of highlights
                .entry((field, format!("{}", expr)))
                // add to existing entries
                .and_modify(|v| v.append(&mut entries))
                // or insert brand new entries
                .or_insert(entries);
        };

        let mut cnt = 0;
        match term {
            Term::String(s, _) => {
                if let Some(entries) = highlighter.highlight_token(s) {
                    cnt = entries.len();
                    process_entries(field, entries, highlights);
                }
            }
            Term::Wildcard(s, _) => {
                if let Some(entries) = highlighter.highlight_wildcard(s) {
                    cnt = entries.len();
                    process_entries(field, entries, highlights);
                }
            }
            Term::Fuzzy(s, d, _) => {}
            Term::ProximityChain(v) => {}

            Term::ParsedArray(_, _) => {}
            Term::UnparsedArray(_, _) => {}
            Term::Range(_, _, _) => {}
            Term::Null => {}
        }

        cnt > 0
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
        name!(query_clause, String),
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

    let qh = QueryHighligther::new(&index, document.0, fields, query);
    let highlights = qh.highlight();

    highlights
        .into_iter()
        .map(|((field, expr), entries)| {
            entries.into_iter().map(move |(term, entry)| {
                (
                    field.field.clone(),
                    term,
                    entry.type_.clone(),
                    entry.position as i32,
                    entry.start_offset as i64,
                    entry.end_offset as i64,
                    expr.clone(),
                )
            })
        })
        .flatten()
        .collect::<Vec<_>>()
        .into_iter()
}
