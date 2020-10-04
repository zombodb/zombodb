use crate::highlighting::document_highlighter::*;
use crate::query_parser::ast::{Expr, QualifiedField, Term};
use pgx::*;
use pgx::{JsonB, PgRelation};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

struct QueryHighligther<'a> {
    query: Expr<'a>,
    highlighters: HashMap<&'a str, DocumentHighlighter<'a>>,
    index: &'a PgRelation,
}

impl<'a> QueryHighligther<'a> {
    pub fn new(
        index: &'a PgRelation,
        mut document: serde_json::Value,
        fields: &HashSet<&'a str>,
        query: Expr<'a>,
    ) -> Self {
        let mut highlighters = HashMap::new();
        let document = document.as_object_mut().expect("document not an object");

        fields.iter().for_each(|field| {
            if let Some(value) = document.remove(*field) {
                // for now, assume field value is a string and that it exists
                let value = match value {
                    Value::String(s) => s,
                    _ => {
                        serde_json::to_string(&value).expect("unable to convert value to a string")
                    }
                };
                let dh = DocumentHighlighter::new(index, field, &value);
                highlighters.insert(*field, dh);
            }
        });

        QueryHighligther {
            highlighters,
            query,
            index,
        }
    }

    pub fn highlight(&'a self) -> Vec<(String, String, String, i32, i64, i64, String)> {
        let mut highlights = HashMap::new();
        if self.walk_expression(&self.query, &mut highlights) {
            highlights
                .into_iter()
                .map(|((field, expr), entries)| {
                    entries.into_iter().map(move |(term, entry)| {
                        (
                            field.field.to_owned(),
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
            vec![]
        }
    }

    fn walk_expression(
        &'a self,
        expr: &'a Expr<'a>,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) -> bool {
        match expr {
            Expr::Not(e) => !self.walk_expression(e.as_ref(), highlights),

            Expr::WithList(v) | Expr::AndList(v) => {
                let mut did_highlight = false;

                let mut tmp_highlights = HashMap::new();
                for e in v {
                    did_highlight = self.walk_expression(e, &mut tmp_highlights);
                    if !did_highlight {
                        break;
                    }
                }

                if did_highlight {
                    highlights.extend(tmp_highlights);
                }

                did_highlight
            }

            Expr::OrList(v) => {
                let mut did_highlight = false;

                for e in v {
                    let mut tmp_highlights = HashMap::new();
                    if self.walk_expression(e, &mut tmp_highlights) {
                        highlights.extend(tmp_highlights);
                        did_highlight = true;
                    }
                }
                did_highlight
            }

            Expr::Linked(_i, e) => self.walk_expression(e.as_ref(), highlights),

            Expr::Nested(_, e) => self.walk_expression(e.as_ref(), highlights),

            Expr::Subselect(_, _) => panic!("subselect not supported yet"),
            Expr::Expand(_, _, _) => panic!("expand not supported yet"),
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

            Expr::MoreLikeThis(_, _) => unimplemented!(),
            Expr::FuzzyLikeThis(_, _) => unimplemented!(),
        }
    }

    fn highlight_term_scan<F: Fn(&str, &str) -> bool>(
        &'a self,
        highlighter: &'a DocumentHighlighter,
        field: QualifiedField,
        expr: &'a Expr<'a>,
        term: &Term,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
        eval: F,
    ) -> bool {
        let mut cnt = 0;

        match term {
            Term::String(s, _) => {
                if let Some(entries) = highlighter.highlight_token_scan(s, eval) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
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
        expr: &'a Expr<'a>,
        term: &Term,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) -> bool {
        let mut cnt = 0;
        match term {
            Term::MatchAll => {
                // do nothing
            }
            Term::String(s, _) => {
                if let Some(entries) = highlighter.highlight_token(s) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Phrase(s, _) | Term::PhraseWithWildcard(s, _) | Term::PhrasePrefix(s, _) => {
                if let Some(entries) =
                    highlighter.highlight_phrase(self.index, &field.field_name(), s)
                {
                    // try to highlight the phrase
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                } else if let Some(entries) = highlighter.highlight_token(s) {
                    // that didn't work, so try to highlight the phrase as if it were a single token
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Wildcard(s, _) | Term::Prefix(s, _) => {
                if let Some(entries) = highlighter.highlight_wildcard(s) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Regex(r, _) => {
                if let Some(entries) = highlighter.highlight_regex(r) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Fuzzy(s, d, _) => {
                if let Some(entries) = highlighter.highlight_fuzzy(s, *d) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Range(_, _, _) => {
                // TODO:  Do we support highlighting ranges, and then how do we deal with
                //        various data types?
            }
            Term::ProximityChain(v) => {
                if let Some(entries) = highlighter.highlight_proximity(v) {
                    cnt = entries.len();
                    QueryHighligther::process_entries(expr, &field, entries, highlights);
                }
            }

            Term::ParsedArray(v, _) => {
                for t in v {
                    if let Some(entries) = highlighter.highlight_term(t) {
                        cnt += entries.len();
                        QueryHighligther::process_entries(expr, &field, entries, highlights);
                    }
                }
            }
            Term::UnparsedArray(s, _) => {
                let mut term_vec = Vec::new();
                for t in s
                    .split(|c: char| c.is_whitespace() || ",\"'[]".contains(c))
                    .filter(|v| !v.is_empty())
                    .into_iter()
                {
                    term_vec.push(Term::String(t, None))
                }
                for t in term_vec {
                    if let Some(entries) = highlighter.highlight_term(&t) {
                        cnt = entries.len() + cnt;
                        QueryHighligther::process_entries(expr, &field, entries, highlights);
                    }
                }
            }
            Term::Null => {}
        }
        cnt > 0
    }

    fn process_entries(
        expr: &'a Expr<'a>,
        field: &QualifiedField,
        mut entries: Vec<(String, &'a TokenEntry)>,
        highlights: &mut HashMap<(QualifiedField, String), Vec<(String, &'a TokenEntry)>>,
    ) {
        highlights
            // fir this field in our map of highlights
            .entry((field.clone(), format!("{}", expr)))
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
    let query = Expr::from_str(&index, "zdb_all", query_string, &mut used_fields)
        .expect("failed to parse query");

    let qh = QueryHighligther::new(&index, document.0, &used_fields, query);
    qh.highlight().into_iter()
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::highlighting::query_highlighter::QueryHighligther;
    use crate::query_parser::ast::Expr;
    use pgx::*;
    use serde_json::*;
    use std::collections::HashSet;

    // #[pg_test]
    // #[initialize(es = true)]
    // fn varchar() {
    //     let highlights = make_query_highlighter(
    //         "varchar",
    //         json! {{
    //             "varchar": "beer"
    //         }},
    //         "varchar:beer",
    //     )
    //     .highlight();
    //
    //     assert_vec(
    //         highlights,
    //         vec![("varchar", "beer", "<ALPHANUM>", 0, 0, 4, "varchar:\"beer\"")],
    //     )
    // }

    #[pg_test]
    #[initialize(es = true)]
    fn text() {
        let highlights = highlight_document_with_query(
            "text",
            json! {{
                "text": "beer"
            }},
            "text:beer",
        );

        assert_vec(
            highlights,
            vec![("text", "beer", "<ALPHANUM>", 0, 1, 5, "text:\"beer\"")],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn regex() {
        let highlights = highlight_document_with_query(
            "text",
            json! {{
                "regex": "man"
            }},
            "regex:~'^m.*$'",
        );

        assert_vec(
            highlights,
            vec![("regex", "man", "<ALPHANUM>", 0, 1, 4, "regex:~\"^m.*$\"")],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn parsed_array_without_quotes() {
        let highlights = highlight_document_with_query(
            "text",
            json! {{
                "p_array": "a b c d e f g h i j"
            }},
            "p_array:[a,c,e,g,i]",
        );

        assert_vec(
            highlights,
            vec![
                (
                    "p_array",
                    "a",
                    "<ALPHANUM>",
                    0,
                    1,
                    2,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "c",
                    "<ALPHANUM>",
                    2,
                    5,
                    6,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "e",
                    "<ALPHANUM>",
                    4,
                    9,
                    10,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "g",
                    "<ALPHANUM>",
                    6,
                    13,
                    14,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "i",
                    "<ALPHANUM>",
                    8,
                    17,
                    18,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
            ],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn parsed_array_with_quotes() {
        let highlights = highlight_document_with_query(
            "text",
            json! {{
                "p_array": "a b c d e f g h i j"
            }},
            "p_array:['a','c','e','g','i']",
        );

        assert_vec(
            highlights,
            vec![
                (
                    "p_array",
                    "a",
                    "<ALPHANUM>",
                    0,
                    1,
                    2,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "c",
                    "<ALPHANUM>",
                    2,
                    5,
                    6,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "e",
                    "<ALPHANUM>",
                    4,
                    9,
                    10,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "g",
                    "<ALPHANUM>",
                    6,
                    13,
                    14,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "i",
                    "<ALPHANUM>",
                    8,
                    17,
                    18,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
            ],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn unparsed_array() {
        let highlights = highlight_document_with_query(
            "text",
            json! {{
                "unpar_array": "a, 1, \"bob\" ,b , 2, \"larry\",    c, 3, david selph"
            }},
            "unpar_array:[[a, b, c]]",
        );

        assert_vec(
            highlights,
            vec![
                (
                    "unpar_array",
                    "a",
                    "<ALPHANUM>",
                    0,
                    1,
                    2,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "b",
                    "<ALPHANUM>",
                    3,
                    16,
                    17,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "c",
                    "<ALPHANUM>",
                    6,
                    37,
                    38,
                    "unpar_array:[[a, b, c]]",
                ),
            ],
        )
    }

    #[pg_test]
    #[initialize(es = true)]
    fn unparsed_array_more_complex() {
        let highlights = highlight_document_with_query(
            "text",
            json! {{
                "unpar_array": "a, 1, \"bob\" ,b _ 2, \"larry\",    c~ 3, david selph"
            }},
            "unpar_array:[[a, b, c]]",
        );

        assert_vec(
            highlights,
            vec![
                (
                    "unpar_array",
                    "a",
                    "<ALPHANUM>",
                    0,
                    1,
                    2,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "b",
                    "<ALPHANUM>",
                    3,
                    16,
                    17,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "c",
                    "<ALPHANUM>",
                    6,
                    37,
                    38,
                    "unpar_array:[[a, b, c]]",
                ),
            ],
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

    fn highlight_document_with_query<'a>(
        table: &'a str,
        document: serde_json::Value,
        query_string: &'a str,
    ) -> Vec<(String, String, String, i32, i64, i64, String)> {
        let relation = start_table_and_index(table);
        let (query, used_fields) = make_query(&relation, query_string);
        QueryHighligther::new(&relation, document, &used_fields, query).highlight()
    }

    fn make_query<'a>(relation: &PgRelation, input: &'a str) -> (Expr<'a>, HashSet<&'a str>) {
        let mut used_fields = HashSet::new();
        let query = Expr::from_str(relation, "zdb_all", input, &mut used_fields)
            .expect("failed to parse ZDB Query");

        (query, used_fields)
    }

    fn start_table_and_index(title: &str) -> PgRelation {
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

        unsafe { PgRelation::open_with_name(&indexname).expect("failed to open index relation") }
    }
}
