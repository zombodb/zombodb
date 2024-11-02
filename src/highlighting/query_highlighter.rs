use crate::access_method::options::ZDBIndexOptions;
use crate::highlighting::document_highlighter::*;
use crate::utils::{find_zdb_index, get_highlight_analysis_info, has_date_subfield};
use crate::zql::ast::{Expr, IndexLink, QualifiedField, Term};
use once_cell::sync::Lazy;
use pgrx::prelude::*;
use pgrx::{JsonB, PgRelation, *};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

struct QueryHighlighter;

impl QueryHighlighter {
    pub fn highlight<'a>(
        index: &PgRelation,
        document: &'a serde_json::Value,
        fields: &HashSet<(String, Option<String>, Option<String>, bool)>,
        query: &Expr<'a>,
        highlighters: &'a mut HashMap<String, Vec<DocumentHighlighter<'a>>>,
    ) -> impl std::iter::Iterator<
        Item = (
            String,
            i32,
            &'a Cow<'a, str>,
            Cow<'a, str>,
            i32,
            i64,
            i64,
            String,
        ),
    > + 'a {
        let document = document.as_object().expect("document not an object");

        fields
            .iter()
            .for_each(|(base_field, field_type, index_analyzer, is_date)| {
                if let Some(value) = document.get(base_field) {
                    for ((fieldname, _), highlighter) in DocumentHighlighter::from_json(
                        index,
                        base_field,
                        value,
                        0,
                        field_type,
                        *is_date,
                        index_analyzer,
                    ) {
                        highlighters
                            .entry(fieldname)
                            .or_default()
                            .push(highlighter);
                    }
                }
            });

        let mut highlights: Vec<((QualifiedField, String), Vec<(&Cow<'_, str>, &TokenEntry)>)> =
            Default::default();
        let result: Box<
            dyn std::iter::Iterator<
                Item = (
                    String,
                    i32,
                    &'a Cow<'a, str>,
                    Cow<'a, str>,
                    i32,
                    i64,
                    i64,
                    String,
                ),
            >,
        > = if QueryHighlighter::walk_expression(query, &mut highlights, highlighters) {
            Box::new(
                highlights
                    .into_iter()
                    .map(|((field, expr), entries)| {
                        entries.into_iter().map(move |(term, entry)| {
                            (
                                field.field.clone(),
                                entry.array_index as i32,
                                term,
                                entry.type_.clone(),
                                entry.position as i32,
                                entry.start_offset as i64,
                                entry.end_offset as i64,
                                expr.clone(),
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .flatten(),
            )
        } else {
            Box::new(vec![].into_iter())
        };
        result
    }

    fn walk_expression<'a>(
        expr: &Expr<'a>,
        highlights: &mut HighlightCollection<'a>,
        highlighters: &'a HashMap<String, Vec<DocumentHighlighter<'a>>>,
    ) -> bool {
        match expr {
            Expr::Null => unreachable!(),

            Expr::Not(e) => {
                !QueryHighlighter::walk_expression(e.as_ref(), highlights, highlighters)
            }

            Expr::AndList(v) => {
                let mut did_highlight = false;
                let mut tmp_highlights = Default::default();

                for e in v {
                    did_highlight =
                        QueryHighlighter::walk_expression(e, &mut tmp_highlights, highlighters);
                    if !did_highlight {
                        break;
                    }
                }

                if did_highlight {
                    highlights.extend(tmp_highlights);
                }

                did_highlight
            }

            Expr::WithList(v) => {
                let mut did_highlight = false;
                let mut tmp_highlights = HighlightCollection::default();
                let mut array_indexes = HashSet::new();

                for e in v {
                    let mut matches = HighlightCollection::default();
                    did_highlight =
                        QueryHighlighter::walk_expression(e, &mut matches, highlighters);
                    if !did_highlight {
                        break;
                    }

                    if array_indexes.is_empty() {
                        // keep all the array_index values from all matched TokenEntries
                        array_indexes.extend(
                            matches
                                .iter()
                                .flat_map(|matches| matches.1.iter())
                                .map(|e| e.1.array_index),
                        );
                    } else {
                        // keep only the existing array_indexes that are also found in the matched TokenEntries
                        array_indexes.retain(|i| {
                            for m in matches.iter() {
                                for (_, e) in m.1.iter() {
                                    if e.array_index == *i {
                                        return true;
                                    }
                                }
                            }
                            false
                        });
                    }

                    // just remember all the highlights
                    tmp_highlights.extend(matches);
                }

                if did_highlight {
                    // filter the tmp_highlights by those that belong to our refined set of array_indexes
                    // and remember that final set as our known set of highlights
                    highlights.extend(tmp_highlights.into_iter().filter_map(|mut e| {
                        e.1.retain(|e| array_indexes.contains(&e.1.array_index));
                        if e.1.is_empty() {
                            None
                        } else {
                            Some(e)
                        }
                    }));
                }

                did_highlight
            }

            Expr::OrList(v) => {
                let mut did_highlight = false;

                for e in v {
                    let mut tmp_highlights = HighlightCollection::default();
                    if QueryHighlighter::walk_expression(e, &mut tmp_highlights, highlighters) {
                        highlights.extend(tmp_highlights);
                        did_highlight = true;
                    }
                }
                did_highlight
            }

            Expr::Linked(_i, e) => {
                QueryHighlighter::walk_expression(e.as_ref(), highlights, highlighters)
            }

            Expr::Nested(_, e) => {
                QueryHighlighter::walk_expression(e.as_ref(), highlights, highlighters)
            }

            Expr::Subselect(_, e) => {
                QueryHighlighter::walk_expression(e.as_ref(), highlights, highlighters)
            }
            Expr::Expand(_, e, f) => {
                let mut did_highlight =
                    QueryHighlighter::walk_expression(e.as_ref(), highlights, highlighters);
                if let Some(f) = f {
                    did_highlight |=
                        QueryHighlighter::walk_expression(f.as_ref(), highlights, highlighters);
                }
                did_highlight
            }
            Expr::Json(_) => {
                // nothing we can do here to highlight a json query -- we don't know what it is
                false
            }

            Expr::Contains(f, t) | Expr::Eq(f, t) | Expr::Regex(f, t) => match t {
                Term::Range(b, e, boost) => {
                    if let Some(dh) = highlighters.get(&f.field) {
                        let mut did_highlight = false;
                        for dh in dh {
                            let in_range = QueryHighlighter::highlight_term_scan(
                                dh,
                                f.clone(),
                                expr,
                                &Term::String(b, *boost),
                                highlights,
                                dh.ge_func(),
                            ) && QueryHighlighter::highlight_term_scan(
                                dh,
                                f.clone(),
                                expr,
                                &Term::String(e, *boost),
                                highlights,
                                dh.le_func(),
                            );
                            did_highlight |= in_range;
                        }
                        return did_highlight;
                    }
                    false
                }

                _ => {
                    if let Some(dh) = highlighters.get(&f.field) {
                        let mut did_highlight = false;
                        for dh in dh {
                            did_highlight |= QueryHighlighter::highlight_term(
                                dh,
                                f.clone(),
                                expr,
                                t,
                                highlights,
                            );
                        }
                        return did_highlight;
                    }
                    false
                }
            },

            Expr::DoesNotContain(f, t) | Expr::Ne(f, t) => match t {
                Term::Range(b, e, boost) => {
                    if let Some(dh) = highlighters.get(&f.field) {
                        let mut did_highlight = false;
                        for dh in dh {
                            let in_range = QueryHighlighter::highlight_term_scan(
                                dh,
                                f.clone(),
                                expr,
                                &Term::String(b, *boost),
                                highlights,
                                dh.ge_func(),
                            ) && QueryHighlighter::highlight_term_scan(
                                dh,
                                f.clone(),
                                expr,
                                &Term::String(e, *boost),
                                highlights,
                                dh.le_func(),
                            );
                            did_highlight |= !in_range;
                        }
                        return did_highlight;
                    }
                    false
                }

                _ => {
                    if let Some(dh) = highlighters.get(&f.field) {
                        let mut did_highlight = false;
                        for dh in dh {
                            did_highlight |= !QueryHighlighter::highlight_term(
                                dh,
                                f.clone(),
                                expr,
                                t,
                                highlights,
                            );
                        }
                        return did_highlight;
                    }
                    false
                }
            },

            Expr::Gt(f, t) => {
                if let Some(dh) = highlighters.get(&f.field) {
                    let mut did_highlight = false;
                    for dh in dh {
                        did_highlight |= QueryHighlighter::highlight_term_scan(
                            dh,
                            f.clone(),
                            expr,
                            t,
                            highlights,
                            dh.gt_func(),
                        );
                    }
                    return did_highlight;
                }
                false
            }
            Expr::Lt(f, t) => {
                if let Some(dh) = highlighters.get(&f.field) {
                    let mut did_highlight = false;
                    for dh in dh {
                        did_highlight |= QueryHighlighter::highlight_term_scan(
                            dh,
                            f.clone(),
                            expr,
                            t,
                            highlights,
                            dh.lt_func(),
                        );
                    }
                    return did_highlight;
                }
                false
            }
            Expr::Gte(f, t) => {
                if let Some(dh) = highlighters.get(&f.field) {
                    let mut did_highlight = false;
                    for dh in dh {
                        did_highlight |= QueryHighlighter::highlight_term_scan(
                            dh,
                            f.clone(),
                            expr,
                            t,
                            highlights,
                            dh.ge_func(),
                        );
                    }
                    return did_highlight;
                }
                false
            }
            Expr::Lte(f, t) => {
                if let Some(dh) = highlighters.get(&f.field) {
                    let mut did_highlight = false;
                    for dh in dh {
                        did_highlight |= QueryHighlighter::highlight_term_scan(
                            dh,
                            f.clone(),
                            expr,
                            t,
                            highlights,
                            dh.le_func(),
                        );
                    }
                    return did_highlight;
                }
                false
            }

            Expr::MoreLikeThis(_, _) => {
                // can't highlight this
                false
            }
            Expr::FuzzyLikeThis(_, _) => {
                // can't highlight this
                false
            }
            Expr::Matches(_, _) => {
                // can't highlight this
                false
            }
        }
    }

    fn highlight_term_scan<'a, F: Fn(&str, &str) -> bool>(
        highlighter: &'a DocumentHighlighter<'a>,
        field: QualifiedField,
        expr: &Expr<'a>,
        term: &Term,
        highlights: &mut HighlightCollection<'a>,
        eval: F,
    ) -> bool {
        let mut cnt = 0;

        match term {
            Term::String(s, _) | Term::Phrase(s, _) => {
                if let Some(entries) = highlighter.highlight_token_scan(s, eval) {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            _ => panic!("cannot highlight using scans for {:?}", expr),
        }

        cnt > 0
    }

    fn highlight_term<'a>(
        highlighter: &'a DocumentHighlighter<'a>,
        field: QualifiedField,
        expr: &Expr<'a>,
        term: &Term,
        highlights: &mut HighlightCollection<'a>,
    ) -> bool {
        let mut cnt = 0;
        match term {
            Term::MatchAll => {
                if let Some(entries) = highlighter.highlight_regex(".*") {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::MatchNone => {
                // do nothing
            }
            Term::String(s, _) => {
                if let Some(entries) = highlighter.highlight_token(s) {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Phrase(s, _) | Term::PhraseWithWildcard(s, _) | Term::PhrasePrefix(s, _) => {
                if let Some(entries) = highlighter.highlight_phrase(&field.field_name(), s) {
                    // try to highlight the phrase
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                } else if let Some(entries) = highlighter.highlight_token(s) {
                    // that didn't work, so try to highlight the phrase as if it were a single token
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Wildcard(s, _) | Term::Prefix(s, _) => {
                if let Some(entries) = highlighter.highlight_wildcard(s) {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Regex(r, _) => {
                if let Some(entries) = highlighter.highlight_regex(r) {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Fuzzy(s, d, _) => {
                if let Some(entries) = highlighter.highlight_fuzzy(s, *d) {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }
            Term::Range(_, _, _) => {
                // TODO:  Do we support highlighting ranges, and then how do we deal with
                //        various data types?
                unreachable!("Term::Range() is handled elsewhere")
            }
            Term::ProximityChain(v) => {
                if let Some(entries) = highlighter.highlight_proximity(v) {
                    cnt = entries.len();
                    QueryHighlighter::process_entries(expr, &field, entries, highlights);
                }
            }

            Term::ParsedArray(v, _) => {
                for t in v {
                    if let Some(entries) = highlighter.highlight_term(t) {
                        cnt += entries.len();
                        QueryHighlighter::process_entries(expr, &field, entries, highlights);
                    }
                }
            }
            Term::UnparsedArray(s, _) => {
                let mut term_vec = Vec::new();
                for t in s
                    .split(|c: char| c.is_whitespace() || ",\"'[]".contains(c))
                    .filter(|v| !v.is_empty())
                {
                    term_vec.push(Term::String(t, None))
                }
                for t in term_vec {
                    if let Some(entries) = highlighter.highlight_term(&t) {
                        cnt += entries.len();
                        QueryHighlighter::process_entries(expr, &field, entries, highlights);
                    }
                }
            }
            Term::Null => {
                if let Some(_entries) = highlighter.highlight_null() {
                    cnt += 1;
                }
            }
        }
        cnt > 0
    }

    fn process_entries<'a>(
        expr: &Expr<'a>,
        field: &QualifiedField,
        entries: HighlightMatches<'a>,
        highlights: &mut HighlightCollection<'a>,
    ) {
        highlights.push(((field.clone(), format!("{expr}")), entries))
    }
}

#[pg_extern(immutable, parallel_safe, name = "highlight_document")]
fn highlight_document_jsonb(
    index: PgRelation,
    document: JsonB,
    query_string: &str,
    dedup_results: default!(bool, true),
) -> TableIterator<(
    name!(field_name, String),
    name!(array_index, i32),
    name!(term, String),
    name!(type, String),
    name!(position, i32),
    name!(start_offset, i64),
    name!(end_offset, i64),
    name!(query_clause, String),
)> {
    let (expr, used_fields) = make_expr(&index, query_string);
    let used_fields = make_used_fields(&index, used_fields);
    highlight_document_internal(index, &document.0, &expr, used_fields, dedup_results)
}

#[pg_extern(immutable, parallel_safe, name = "highlight_document")]
fn highlight_document_json(
    index: PgRelation,
    document: Json,
    query_string: &str,
    dedup_results: default!(bool, true),
) -> TableIterator<(
    name!(field_name, String),
    name!(array_index, i32),
    name!(term, String),
    name!(type, String),
    name!(position, i32),
    name!(start_offset, i64),
    name!(end_offset, i64),
    name!(query_clause, String),
)> {
    let (expr, used_fields) = make_expr(&index, query_string);
    let used_fields = make_used_fields(&index, used_fields);
    highlight_document_internal(index, &document.0, &expr, used_fields, dedup_results)
}

fn make_used_fields(
    index: &PgRelation,
    mut used_fields: HashSet<String>,
) -> &'static HashSet<(String, Option<String>, Option<String>, bool)> {
    static mut USED_FIELDS_CACHE: Lazy<
        HashMap<String, HashSet<(String, Option<String>, Option<String>, bool)>>,
    > = Lazy::new(|| Default::default());

    unsafe {
        let key = used_fields.iter().map(|s| s.as_str()).collect::<String>();
        USED_FIELDS_CACHE.entry(key.clone()).or_insert_with(|| {
            {
                let key = key.clone();
                register_xact_callback(PgXactCallbackEvent::Abort, move || {
                    USED_FIELDS_CACHE.remove(&key);
                });
            }

            {
                let key = key.clone();
                register_xact_callback(PgXactCallbackEvent::Commit, move || {
                    USED_FIELDS_CACHE.remove(&key);
                });
            }

            let zdboptions = ZDBIndexOptions::from_relation(index);
            zdboptions
                .field_lists().values().flat_map(|v| v.iter())
                .for_each(|fieldname| {
                    used_fields.insert(fieldname.field_name());
                });

            used_fields
                .into_iter()
                .map(|field| {
                    let base_field = field.split('.').next().map(|v| v.to_string()).unwrap();
                    let (field_type, _, index_analyzer) =
                        get_highlight_analysis_info(index, &base_field);
                    let is_date = has_date_subfield(index, &base_field);
                    (base_field, field_type, index_analyzer, is_date)
                })
                .collect::<HashSet<_>>()
        })
    }
}

fn highlight_document_internal<'a>(
    index: PgRelation,
    document: &serde_json::Value,
    query: &Expr,
    used_fields: &HashSet<(String, Option<String>, Option<String>, bool)>,
    dedup_results: bool,
) -> TableIterator<
    'a,
    (
        name!(field_name, String),
        name!(array_index, i32),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
        name!(query_clause, String),
    ),
> {
    let mut highlighters = HashMap::new();
    let iter =
        QueryHighlighter::highlight(&index, document, used_fields, query, &mut highlighters).map(
            |(
                field_name,
                array_index,
                term,
                type_,
                position,
                start_offset,
                end_offset,
                query_clause,
            )| {
                (
                    field_name,
                    array_index,
                    term.to_string(),
                    type_.to_string(),
                    position,
                    start_offset,
                    end_offset,
                    query_clause,
                )
            },
        );

    let iter: Box<dyn std::iter::Iterator<Item = _>> = if dedup_results {
        let mut collected = iter.collect::<Vec<_>>();
        collected.sort_by_key(
            |(field_name, array_index, term, type_, position, start_offset, end_offset, _)| {
                (
                    Clone::clone(field_name),
                    *array_index,
                    *position,
                    *start_offset,
                    *end_offset,
                    Clone::clone(term),
                    Clone::clone(type_),
                )
            },
        );

        let mut deduped = Vec::new();
        let mut peekable = collected.into_iter().peekable();
        while let Some(current) = peekable.next() {
            let pos = current.4;
            let field_name = &current.0;
            let array_index = current.1;
            let query_clause = &current.7;

            let current_uniq = (field_name, array_index, query_clause, pos);
            while let Some(next) = peekable.peek() {
                let next_type = &next.3;
                let next_pos = next.4;
                let next_field_name = &next.0;
                let next_array_index = next.1;
                let next_query_clause = &next.7;

                let next_uniq = (
                    next_field_name,
                    next_array_index,
                    next_query_clause,
                    next_pos,
                );

                if next == &current || (current_uniq == next_uniq && next_type == "shingle") {
                    // skip duplicates or the next one if it's a shingle at the same position
                    peekable.next();
                } else {
                    break;
                }
            }
            deduped.push(current);
        }
        Box::new(deduped.into_iter())
    } else {
        Box::new(iter.collect::<Vec<_>>().into_iter())
    };
    TableIterator::new(iter)
}

fn make_expr<'a>(index: &PgRelation, query_string: &'a str) -> (Expr<'a>, HashSet<String>) {
    // select * from zdb.highlight_document('idxbeer', '{"subject":"free beer", "authoremail":"Christi l nicolay"}', '!!subject:beer or subject:fr?? and authoremail:(christi, nicolay)') order by field_name, position;
    let mut used_fields = HashSet::new();
    let (index, options) = find_zdb_index(index).expect("unable to find ZomboDB index");
    let links = IndexLink::from_options(&index, options);
    let expr = Expr::from_str(
        &index,
        "zdb_all",
        query_string,
        &links,
        &None,
        &mut used_fields,
    )
    .expect("failed to parse query");
    let used_fields = used_fields.into_iter().map(|s| s.to_string()).collect();
    (expr, used_fields)
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::highlighting::query_highlighter::{make_used_fields, QueryHighlighter};
    use crate::zql::ast::Expr;
    use pgrx::*;
    use serde_json::*;
    use std::collections::{HashMap, HashSet};

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
            vec![("text", "beer", "<ALPHANUM>", 1, 0, 4, "text:\"beer\"")],
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
            vec![("regex", "man", "<ALPHANUM>", 1, 0, 3, "regex:~\"^m.*$\"")],
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
                    1,
                    0,
                    1,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "c",
                    "<ALPHANUM>",
                    3,
                    4,
                    5,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "e",
                    "<ALPHANUM>",
                    5,
                    8,
                    9,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "g",
                    "<ALPHANUM>",
                    7,
                    12,
                    13,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "i",
                    "<ALPHANUM>",
                    9,
                    16,
                    17,
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
                    1,
                    0,
                    1,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "c",
                    "<ALPHANUM>",
                    3,
                    4,
                    5,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "e",
                    "<ALPHANUM>",
                    5,
                    8,
                    9,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "g",
                    "<ALPHANUM>",
                    7,
                    12,
                    13,
                    "p_array:[\"a\",\"c\",\"e\",\"g\",\"i\"]",
                ),
                (
                    "p_array",
                    "i",
                    "<ALPHANUM>",
                    9,
                    16,
                    17,
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
                    1,
                    0,
                    1,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "b",
                    "<ALPHANUM>",
                    4,
                    13,
                    14,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "c",
                    "<ALPHANUM>",
                    7,
                    32,
                    33,
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
                    1,
                    0,
                    1,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "b",
                    "<ALPHANUM>",
                    4,
                    13,
                    14,
                    "unpar_array:[[a, b, c]]",
                ),
                (
                    "unpar_array",
                    "c",
                    "<ALPHANUM>",
                    7,
                    32,
                    33,
                    "unpar_array:[[a, b, c]]",
                ),
            ],
        )
    }

    fn assert_vec(
        left: Vec<(String, i32, String, String, i32, i64, i64, String)>,
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
                left.2, right.1,
                "term mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.3, right.2,
                "type mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.4, right.3,
                "position mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.5, right.4,
                "start_offset mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.6, right.5,
                "end_offset mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
            assert_eq!(
                left.7, right.6,
                "query_clause mismatch at #{}\n    left={:?}\n   right={:?}",
                i, left, right
            );
        }
    }

    fn highlight_document_with_query(
        table: &str,
        document: serde_json::Value,
        query_string: &str,
    ) -> Vec<(String, i32, String, String, i32, i64, i64, String)> {
        let relation = start_table_and_index(table);
        let (query, used_fields) = make_query(&relation, query_string);
        let used_fields = make_used_fields(&relation, used_fields);

        let mut highlighters = HashMap::new();
        let mut results = QueryHighlighter::highlight(
            &relation,
            &document,
            used_fields,
            &query,
            &mut highlighters,
        )
        .map(|(a, b, c, d, e, f, g, h)| (a, b, c.to_string(), d.to_string(), e, f, g, h))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
        results.sort_by_key(|x| x.4);
        results
    }

    fn make_query<'a>(relation: &PgRelation, input: &'a str) -> (Expr<'a>, HashSet<String>) {
        let mut used_fields = HashSet::new();
        let query = Expr::from_str(relation, "zdb_all", input, &vec![], &None, &mut used_fields)
            .expect("failed to parse ZDB Query");
        let used_fields = used_fields.into_iter().map(|s| s.to_string()).collect();
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
        Spi::run(create_table).expect("SPI failed");
        let create_index = &format!(
            "CREATE INDEX {index} ON {table} USING zombodb (({table}.*))",
            index = indexname,
            table = tablename
        );
        Spi::run(create_index).expect("SPI failed");

        unsafe { PgRelation::open_with_name(&indexname).expect("failed to open index relation") }
    }
}
