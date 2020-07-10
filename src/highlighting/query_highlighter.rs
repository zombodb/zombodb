use crate::highlighting::document_highlighter::*;
use crate::query_parser::ast::*;
use pgx::*;
use pgx::{JsonB, PgRelation};
use std::collections::{HashMap, HashSet};

struct QueryHighligther<'a> {
    query: Box<Expr<'a>>,
    highlighters: HashMap<&'a str, DocumentHighlighter<'a>>,
}

impl<'a> QueryHighligther<'a> {
    pub fn new(
        index: &PgRelation,
        document: serde_json::Value,
        fields: &HashSet<&'a str>,
        query: Box<Expr<'a>>,
    ) -> Self {
        let mut highlighters = HashMap::new();

        fields.iter().for_each(|field| {
            if let Some(value) = document
                .as_object()
                .expect("document not an object")
                .get(*field)
            {
                // for now, assume field value is a string and that it exists
                let value =
                    serde_json::to_string(value).expect("unable to convert value to a string");

                let mut dh = DocumentHighlighter::new();
                dh.analyze_document(index, field, &value);

                highlighters.insert(*field, dh);
            }
        });

        QueryHighligther {
            highlighters,
            query,
        }
    }

    pub fn highlight(
        &'a self,
    ) -> Option<HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>> {
        let mut highlights = HashMap::new();
        if self.walk_expression(&self.query, &mut highlights) {
            Some(highlights)
        } else {
            None
        }
    }

    fn walk_expression(
        &'a self,
        expr: &'a Box<Expr<'a>>,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) -> bool {
        match expr.as_ref() {
            Expr::Not(e) => !self.walk_expression(e, highlights),

            Expr::With(l, r) | Expr::And(l, r) => {
                let mut did_highlight;
                let mut tmp_highlights = HashMap::new();

                did_highlight = self.walk_expression(l, &mut tmp_highlights);
                if did_highlight {
                    did_highlight &= self.walk_expression(r, &mut tmp_highlights);
                }

                if did_highlight {
                    highlights.extend(tmp_highlights);
                }
                did_highlight
            }

            Expr::Or(l, r) => {
                let mut did_highlight = false;

                let mut tmp_highlights = HashMap::new();
                if self.walk_expression(l, &mut tmp_highlights) {
                    highlights.extend(tmp_highlights);
                    did_highlight = true;
                }

                let mut tmp_highlights = HashMap::new();
                if self.walk_expression(r, &mut tmp_highlights) {
                    highlights.extend(tmp_highlights);
                    did_highlight = true;
                }
                did_highlight
            }

            Expr::Subselect(_, _) => panic!("subselect not supported yet"),
            Expr::Expand(_, _) => panic!("expand not supported yet"),
            Expr::Json(_) => panic!("json not supported yet"),

            Expr::Contains(f, t) | Expr::Eq(f, t) => {
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    return self.highlight_term(dh, f.clone(), expr, t, highlights);
                }
                false
            }
            Expr::DoesNotContain(f, t) | Expr::Ne(f, t) => {
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    return !self.highlight_term(dh, f.clone(), expr, t, highlights);
                }
                false
            }

            Expr::Gt(f, t) => {
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    return self.highlight_term_scan(
                        dh,
                        f.clone(),
                        expr,
                        t,
                        highlights,
                        dh.gt_func(),
                    );
                }
                false
            }
            Expr::Lt(f, t) => {
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    return self.highlight_term_scan(
                        dh,
                        f.clone(),
                        expr,
                        t,
                        highlights,
                        dh.lt_func(),
                    );
                }
                false
            }
            Expr::Gte(f, t) => {
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    return self.highlight_term_scan(
                        dh,
                        f.clone(),
                        expr,
                        t,
                        highlights,
                        dh.ge_func(),
                    );
                }
                false
            }
            Expr::Lte(f, t) => {
                if let Some(dh) = self.highlighters.get(f.field.as_str()) {
                    return self.highlight_term_scan(
                        dh,
                        f.clone(),
                        expr,
                        t,
                        highlights,
                        dh.le_func(),
                    );
                }
                false
            }

            Expr::Range(_, _, _) => unimplemented!(),

            Expr::Regex(_, _) => unimplemented!(),
            Expr::MoreLikeThis(_, _) => unimplemented!(),
            Expr::FuzzyLikeThis(_, _) => unimplemented!(),
        }
    }

    fn highlight_term_scan<F: Fn(&str, &str) -> bool>(
        &'a self,
        highlighter: &'a DocumentHighlighter,
        field: QualifiedField,
        expr: &'a Box<Expr<'a>>,
        term: &Term,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
        eval: F,
    ) -> bool {
        let mut cnt = 0;

        match term {
            Term::String(s, _) => {
                if let Some(entries) = highlighter.highlight_token_scan(s, eval) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }
            _ => panic!("cannot highlight using scans for {}", expr),
        }

        cnt > 0
    }

    fn highlight_term(
        &'a self,
        highlighter: &'a DocumentHighlighter,
        field: QualifiedField,
        expr: &'a Box<Expr<'a>>,
        term: &Term,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) -> bool {
        let mut cnt = 0;
        match term {
            Term::String(s, _) => {
                if let Some(entries) = highlighter.highlight_token(s) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }
            Term::Wildcard(s, _) => {
                if let Some(entries) = highlighter.highlight_wildcard(s) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }
            Term::Fuzzy(s, d, _) => {
                //todo ask if we want to make fuzzy take a usize or a u8
                if let Some(entries) = highlighter.highlight_fuzzy(s, *d as usize) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }
            Term::ProximityChain(v) => {}

            Term::ParsedArray(_, _) => {}
            Term::UnparsedArray(_, _) => {}
            Term::Null => {}
        }
        cnt > 0
    }

    fn process_entries(
        expr: &'a Box<Expr<'a>>,
        field: QualifiedField,
        mut entries: Vec<(String, &'a TokenEntry)>,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) {
        highlights
            // fir this field in our map of highlights
            .entry((field, format!("{}", expr)))
            // add to existing entries
            .and_modify(|v| v.append(&mut entries))
            // or insert brand new entries
            .or_insert(entries);
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
    let mut used_fields = HashSet::new();
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
        &mut used_fields,
    )
    .expect("failed to parse query");

    let qh = QueryHighligther::new(&index, document.0, &used_fields, query);
    let highlights = qh.highlight();

    highlights
        .unwrap_or_default()
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
