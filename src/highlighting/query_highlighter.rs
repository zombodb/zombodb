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

    pub fn highlight(&'a self) -> Vec<(String, String, String, i32, i64, i64, String)> {
        let mut highlights = HashMap::new();
        if self.walk_expression(&self.query, &mut highlights) {
            pgx::info!("{:?}", highlights);
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
        } else {
            pgx::info!("walk_exprssion returned false");
            vec![]
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

            Expr::Contains(f, t) | Expr::Eq(f, t) | Expr::Regex(f, t) => {
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
            Term::Regex(r, _) => {
                if let Some(entries) = highlighter.highlight_regex(r) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }
            Term::Fuzzy(s, d, _) => {
                if let Some(entries) = highlighter.highlight_fuzzy(s, *d) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }
            Term::ProximityChain(v) => {
                if let Some(entries) = highlighter.highlight_proximity(v) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, field, entries, highlights);
                }
            }

            //todo working on this one
            Term::ParsedArray(v, _) => {
                // for t in v {
                //     if let Some(entries) = highlighter.highlight_term(t) {
                //         let field = field.cnt = entries.len() + cnt;
                //         QueryHighligther::process_entries(expr, field, entries, highlights);
                //     }
                // }
            }
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
    qh.highlight().into_iter()
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::highlighting::query_highlighter::QueryHighligther;
    use crate::query_parser::ast::{Expr, QualifiedIndex};
    use pgx::*;
    use serde_json::*;
    use std::collections::HashSet;

    #[pg_test]
    #[initialize(es = true)]
    fn varchar() {
        let highlights = make_query_highlighter(
            "varchar",
            json! {{
                "varchar": "beer"
            }},
            "varchar:beer",
        )
        .highlight();

        assert_vec(
            highlights,
            vec![("varchar", "beer", "<ALPHANUM>", 0, 0, 4, "varchar:\"beer\"")],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn text() {
        let highlights = make_query_highlighter(
            "text",
            json! {{
                "text": "beer"
            }},
            "text:beer",
        )
        .highlight();

        assert_vec(
            highlights,
            vec![("text", "beer", "<ALPHANUM>", 0, 1, 5, "text:\"beer\"")],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn regex() {
        let highlights = make_query_highlighter(
            "text",
            json! {{
                "regex": "man"
            }},
            "regex:~'^m.*$'",
        )
        .highlight();

        assert_vec(
            highlights,
            vec![("regex", "man", "<ALPHANUM>", 0, 1, 4, "regex:~\"^m.*$\"")],
        )
    }

    fn assert_vec(
        left: Vec<(String, String, String, i32, i64, i64, String)>,
        right: Vec<(&str, &str, &str, i32, i64, i64, &str)>,
    ) {
        assert_eq!(left.len(), right.len(), "left/right lengths are not equal");
        for (i, (left, right)) in left.into_iter().zip(right).enumerate() {
            assert_eq!(
                left.0, right.0,
                "fieldname mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.1, right.1,
                "term mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.2, right.2,
                "type mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.3, right.3,
                "position mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.4, right.4,
                "start_offset mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.5, right.5,
                "end_offset mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.6, right.6,
                "query_clause mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
        }
    }

    fn make_query_highlighter<'a>(
        table: &'a str,
        document: serde_json::Value,
        query_string: &'a str,
    ) -> QueryHighligther<'a> {
        let (relation, table, index) = start_table_and_index(table);
        let (query, used_fields) = make_query(table, index, query_string);
        pgx::info!("used_fields={:?}", used_fields);
        pgx::info!("query={:?}", query);
        QueryHighligther::new(&relation, document, &used_fields, query)
    }

    fn make_query(table: String, index: String, input: &str) -> (Box<Expr>, HashSet<&str>) {
        let mut used_fields = HashSet::new();
        let query = Expr::from_str(
            QualifiedIndex {
                schema: None,
                table,
                index,
            },
            "_zdb_all",
            input,
            &mut used_fields,
        )
        .expect("failed to parse ZDB Query");

        (query, used_fields)
    }

    fn start_table_and_index(title: &str) -> (PgRelation, String, String) {
        let tablename = format!("test_highlighting_{}", title);
        let indexname = format!("idxtest_highlighting_{}", title);
        let create_table = &format!(
            r#"CREATE TABLE {} (
                "bigint" bigint,      -- maps to Elasticsearch 'long' type
                "varchar" varchar,  -- maps to Elasticserach 'keyword' type
                "text" text,      -- maps to Elasticsearch 'text' type
                "integer" integer  -- maps to Elasticsdarch 'int' type
            )"#,
            tablename,
        );
        Spi::run(create_table);
        let create_index = &format!(
            "CREATE INDEX {index} ON {table} USING zombodb (({table}.*))",
            index = indexname,
            table = tablename
        );
        Spi::run(create_index);

        (
            unsafe {
                PgRelation::open_with_name(&indexname).expect("failed to open index relation")
            },
            tablename,
            indexname,
        )
    }
}
