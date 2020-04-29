use crate::elasticsearch::analyze::*;
use crate::elasticsearch::Elasticsearch;
use levenshtein::*;
use pgx::PgRelation;
use pgx::*;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TokenEntry {
    type_: String,
    position: u32,
    start_offset: u64,
    end_offset: u64,
}

pub struct DocumentHighlighter {
    lookup: HashMap<String, Vec<TokenEntry>>,
}

impl DocumentHighlighter {
    pub fn new() -> Self {
        DocumentHighlighter {
            lookup: HashMap::with_capacity(150),
        }
    }

    pub fn analyze_document(&mut self, index: &PgRelation, field: &str, text: &str) {
        let es = Elasticsearch::new(index);
        let results = es
            .analyze_with_field(field, text)
            .execute()
            .expect("failed to analyze text for highlighting");

        for token in results.tokens {
            let entry = self
                .lookup
                .entry(token.token)
                .or_insert(Vec::with_capacity(5));

            entry.push(TokenEntry {
                type_: token.type_,
                position: token.position as u32,
                start_offset: token.start_offset as u64,
                end_offset: token.end_offset as u64,
            });
        }
    }

    pub fn highlight_token(&self, token: &str) -> Option<&Vec<TokenEntry>> {
        self.lookup.get(token)
    }

    pub fn highlight_wildcard(&self, token: &str) -> Option<Vec<(String, &TokenEntry)>> {
        let _char_looking_for_asterisk = '*';
        let _char_looking_for_question = '?';
        let mut new_regex = String::from("^");
        for char in token.chars() {
            if char == _char_looking_for_question {
                new_regex.push('.')
            } else if char == _char_looking_for_asterisk {
                new_regex.push('.');
                new_regex.push(char);
            } else {
                new_regex.push(char);
            }
        }
        new_regex.push_str("$");
        self.highlight_regex(new_regex.deref())
    }

    pub fn highlight_regex(&self, regex: &str) -> Option<Vec<(String, &TokenEntry)>> {
        let regex = Regex::new(regex).unwrap();
        let mut result = Vec::new();
        for (key, token_entries) in self.lookup.iter() {
            if regex.is_match(key.as_str()) {
                for token_entry in token_entries {
                    result.push((key.clone(), token_entry));
                }
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn highlight_fuzzy(
        &self,
        fuzzy_key: &str,
        prefix: i32,
        fuzzy: i32,
    ) -> Option<Vec<(&String, &TokenEntry)>> {
        let mut result = Vec::new();
        let prefix = &fuzzy_key[0..prefix as usize];
        for (token, token_entries) in self.lookup.iter() {
            if token.starts_with(prefix.deref()) {
                if fuzzy > levenshtein(token, fuzzy_key) as i32 {
                    for token_entry in token_entries {
                        result.push((token, token_entry));
                    }
                }
            };
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn highlight_phrase(
        &self,
        index: PgRelation,
        field: &str,
        phrase_str: &str,
    ) -> Option<Vec<(String, &TokenEntry)>> {
        if phrase_str.is_empty() {
            return None;
        }

        let phrase = analyze_with_field(index, field, phrase_str)
            .map(|parts| parts.1)
            .collect::<Vec<String>>();

        self.highlight_phrase_vector(phrase)
    }

    // 'drinking green beer is better than drinking yellow beer which wine is worse than drinking yellow wine'
    //                                     ^^^^^^^^^^^^^^^                          ^^^^^^^^^^^^^^^
    // [ "drinking", "yellow" ]   query= drinking wo/1 yellow
    //
    // query= than w/2 wine
    // query= than wo/2 (wine or beer or cheese or food) w/5 cowbell
    pub fn highlight_phrase_vector(
        &self,
        phrase: Vec<String>,
    ) -> Option<Vec<(String, &TokenEntry)>> {
        if phrase.len() == 1 {
            let token = phrase.get(0).unwrap();
            let result = self.highlight_token(token);
            return match result {
                Some(result) => Some(
                    result
                        .iter()
                        .map(|entry| (token.clone(), entry))
                        .collect::<Vec<(String, &TokenEntry)>>(),
                ),
                None => None,
            };
        }

        let mut pool = HashMap::<&str, &Vec<TokenEntry>>::new();
        for token in &phrase {
            let token_entry = self.highlight_token(token);
            match token_entry {
                Some(vec) => {
                    pool.insert(token, vec);
                }
                None => {
                    return None;
                }
            }
        }

        let mut filtered_pool = HashMap::<&str, HashSet<&TokenEntry>>::new();
        let mut itr = phrase.iter().peekable();
        loop {
            let first = itr.next();
            let second = itr.peek();

            if second.is_none() {
                break;
            }

            let first = first.unwrap();
            let second = second.unwrap();
            let first_vec = pool.get(first.as_str()).unwrap().iter();
            let second_vec = pool.get(second.as_str()).unwrap().iter();
            for token_entry_one in first_vec {
                for token_entry_two in second_vec.clone() {
                    if token_entry_two.position as i32 - token_entry_one.position as i32 == 1 {
                        let entry_list =
                            filtered_pool.entry(first).or_insert_with(|| HashSet::new());
                        entry_list.insert(token_entry_one);

                        let entry_list = filtered_pool
                            .entry(*second)
                            .or_insert_with(|| HashSet::new());
                        entry_list.insert(token_entry_two);

                        break;
                    }
                }
            }
        }

        let mut all_entries = Vec::new();
        for (token, entries) in filtered_pool {
            for entry in entries {
                all_entries.push((token.to_owned(), entry));
            }
        }
        all_entries.sort_by(|a, b| a.1.position.cmp(&b.1.position));
        let mut i = 0;
        loop {
            let entry = all_entries.get(i);
            if entry.is_none() {
                break;
            }
            let mut good_cnt = 0;
            let mut entry = entry.unwrap();
            if entry.0 == *phrase.get(0).unwrap() {
                good_cnt += 1;
                let mut k = 1;
                for j in i + 1..i + phrase.len() {
                    if all_entries.get(j).is_none() {
                        break;
                    }
                    let next = all_entries.get(j).unwrap();
                    if entry.1.position + 1 == next.1.position && next.0 == *phrase.get(k).unwrap()
                    {
                        good_cnt += 1;
                        entry = next;
                    } else {
                        break;
                    }
                    k += 1;
                }
            }
            if good_cnt == phrase.len() {
                i += phrase.len();
            } else {
                all_entries.remove(i);
            }
        }

        if all_entries.is_empty() {
            None
        } else {
            Some(all_entries)
        }
    }
}

#[pg_extern(imutable, parallel_safe)]
fn highlight_term(
    index: PgRelation,
    field_name: &str,
    text: &str,
    token_to_highlight: String,
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
    let mut highlighter = DocumentHighlighter::new();
    highlighter.analyze_document(&index, field_name, text);
    let highlights = highlighter.highlight_token(&token_to_highlight);

    match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.clone().to_owned(),
                    token_to_highlight.clone(),
                    e.type_.clone(),
                    e.position as i32,
                    e.start_offset as i64,
                    e.end_offset as i64,
                )
            })
            .collect::<Vec<(String, String, String, i32, i64, i64)>>()
            .into_iter(),
        None => Vec::<(String, String, String, i32, i64, i64)>::new().into_iter(),
    }
}

#[pg_extern(imutable, parallel_safe)]
fn highlight_phrase(
    index: PgRelation,
    field_name: &str,
    text: &str,
    tokens_to_highlight: &str,
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
    let mut highlighter = DocumentHighlighter::new();
    highlighter.analyze_document(&index, field_name, text);
    let highlights = highlighter.highlight_phrase(index, field_name, tokens_to_highlight);

    match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.clone().to_owned(),
                    String::from(e.0.clone()),
                    String::from(e.1.type_.clone()),
                    e.1.position as i32,
                    e.1.start_offset as i64,
                    e.1.end_offset as i64,
                )
            })
            .collect::<Vec<(String, String, String, i32, i64, i64)>>()
            .into_iter(),
        None => Vec::<(String, String, String, i32, i64, i64)>::new().into_iter(),
    }
}

#[pg_extern(imutable, parallel_safe)]
fn highlight_wildcard(
    index: PgRelation,
    field_name: &str,
    text: &str,
    token_to_highlight: &str,
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
    let mut highlighter = DocumentHighlighter::new();
    highlighter.analyze_document(&index, field_name, text);
    let highlights = highlighter.highlight_wildcard(token_to_highlight);

    match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.clone().to_owned(),
                    String::from(e.0.clone()),
                    String::from(e.1.type_.clone()),
                    e.1.position as i32,
                    e.1.start_offset as i64,
                    e.1.end_offset as i64,
                )
            })
            .collect::<Vec<(String, String, String, i32, i64, i64)>>()
            .into_iter(),
        None => Vec::<(String, String, String, i32, i64, i64)>::new().into_iter(),
    }
}

#[pg_extern(imutable, parallel_safe)]
fn highlight_regex(
    index: PgRelation,
    field_name: &str,
    text: &str,
    token_to_highlight: &str,
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
    let mut highlighter = DocumentHighlighter::new();
    highlighter.analyze_document(&index, field_name, text);
    let highlights = highlighter.highlight_regex(token_to_highlight);

    match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.clone().to_owned(),
                    String::from(e.0.clone()),
                    String::from(e.1.type_.clone()),
                    e.1.position as i32,
                    e.1.start_offset as i64,
                    e.1.end_offset as i64,
                )
            })
            .collect::<Vec<(String, String, String, i32, i64, i64)>>()
            .into_iter(),
        None => Vec::<(String, String, String, i32, i64, i64)>::new().into_iter(),
    }
}

#[pg_extern(imutable, parallel_safe)]
fn highlight_fuzzy(
    index: PgRelation,
    field_name: &str,
    text: &str,
    token_to_highlight: &str,
    prefix: i32,
    fuzzy: i32,
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
    let mut highlighter = DocumentHighlighter::new();
    highlighter.analyze_document(&index, field_name, text);
    let highlights = highlighter.highlight_fuzzy(token_to_highlight, prefix, fuzzy);

    match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.clone().to_owned(),
                    String::from(e.0.clone()),
                    String::from(e.1.type_.clone()),
                    e.1.position as i32,
                    e.1.start_offset as i64,
                    e.1.end_offset as i64,
                )
            })
            .collect::<Vec<(String, String, String, i32, i64, i64)>>()
            .into_iter(),
        None => Vec::<(String, String, String, i32, i64, i64)>::new().into_iter(),
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_term() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_term('idxtest_highlighting', 'test_field', 'it is a test and it is a good one', 'it') order by position;",
                None,
                None,
            );

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // name       | it   | <ALPHANUM> |        0 |            0 |          2
            // name       | it   | <ALPHANUM> |        5 |           17 |         19
            let expect = vec![
                ("<ALPHANUM>", "it", 0, 0, 2),
                ("<ALPHANUM>", "it", 5, 17, 19),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_phrase() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_phrase('idxtest_highlighting', 'test_field', 'it is a test and it is a good one', 'it is a') order by position;",
                None,
                None,
            );

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | it   | <ALPHANUM> |        0 |            0 |          2
            // test_field | is   | <ALPHANUM> |        1 |            3 |          5
            // test_field | a    | <ALPHANUM> |        2 |            6 |          7
            // test_field | it   | <ALPHANUM> |        5 |           17 |         19
            // test_field | is   | <ALPHANUM> |        6 |           20 |         22
            // test_field | a    | <ALPHANUM> |        7 |           23 |         24
            let expect = vec![
                ("<ALPHANUM>", "it", 0, 0, 2),
                ("<ALPHANUM>", "is", 1, 3, 5),
                ("<ALPHANUM>", "a", 2, 6, 7),
                ("<ALPHANUM>", "it", 5, 17, 19),
                ("<ALPHANUM>", "is", 6, 20, 22),
                ("<ALPHANUM>", "a", 7, 23, 24),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_phrase_as_one_word() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_phrase('idxtest_highlighting', 'test_field', 'it is a test and it is a good one', 'it') order by position;",
                None,
                None,
            );

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | it   | <ALPHANUM> |        0 |            0 |          2
            // test_field | it   | <ALPHANUM> |        5 |           17 |         19
            let expect = vec![
                ("<ALPHANUM>", "it", 0, 0, 2),
                ("<ALPHANUM>", "it", 5, 17, 19),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_phrase_with_phrase_not_in_text() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_phrase('idxtest_highlighting', 'test_field', 'it is a test and it is a good one', 'banana') order by position;",
                None,
                None,
            );

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            let expect = vec![];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_wildcard_with_asterisk() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_wildcard('idxtest_highlighting', 'test_field', 'Mom landed a man on the moon', 'm*n') order by position;",
                None,
                None,
            );

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            //  test_field | man  | <ALPHANUM> |        3 |           13 |         16
            //  test_field | moon | <ALPHANUM> |        6 |           24 |         28
            let expect = vec![
                ("<ALPHANUM>", "man", 3, 13, 16),
                ("<ALPHANUM>", "moon", 6, 24, 28),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_wildcard_with_question_mark() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_wildcard('idxtest_highlighting', 'test_field', 'Mom landed a man on the moon', 'm?n') order by position;",
                None,
                None,
            );

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            //  test_field | man  | <ALPHANUM> |        3 |           13 |         16
            let expect = vec![("<ALPHANUM>", "man", 3, 13, 16)];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_wildcard_with_no_match() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_wildcard('idxtest_highlighting', 'test_field', 'Mom landed a man on the moon', 'n*n') order by position;",
                None,
                None,
            );

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            let expect = vec![];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_regex() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_wildcard('idxtest_highlighting', 'test_field', 'Mom landed a man on the moon', '^m.*$') order by position;",
                None,
                None,
            );

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | mom  | <ALPHANUM> |        0 |            0 |          3
            // test_field | man  | <ALPHANUM> |        3 |           13 |         16
            // test_field | moon | <ALPHANUM> |        6 |           24 |         28
            let expect = vec![
                ("<ALPHANUM>", "mom", 0, 0, 3),
                ("<ALPHANUM>", "man", 3, 13, 16),
                ("<ALPHANUM>", "moon", 6, 24, 28),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_fuzzy_correct() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_fuzzy('idxtest_highlighting', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt', 'cot', 1,3) order by position;",
                None,
                None,
            );

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | coal | <ALPHANUM> |        0 |            0 |          4
            // test_field | colt | <ALPHANUM> |        1 |            5 |          9
            // test_field | cot  | <ALPHANUM> |        2 |           10 |         13
            // test_field | co   | <ALPHANUM> |        5 |           26 |         28
            // test_field | cat  | <ALPHANUM> |        8 |           41 |         44
            let expect = vec![
                ("<ALPHANUM>", "coal", 0, 0, 4),
                ("<ALPHANUM>", "colt", 1, 5, 9),
                ("<ALPHANUM>", "cot", 2, 10, 13),
                ("<ALPHANUM>", "co", 5, 26, 28),
                ("<ALPHANUM>", "cat", 8, 41, 44),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test(error = "byte index 4 is out of bounds of `cot`")]
    #[initialize(es = true)]
    fn test_highlighter_fuzzy_with_prefix_wrong() {
        start_table_and_index();
        Spi::connect(|client| {
            let table = client.select(
                "select * from zdb.highlight_fuzzy('idxtest_highlighting', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt', 'cot', 4,3) order by position;",
                None,
                None,
            );

            let expect = vec![];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[initialize(es = true)]
    fn start_table_and_index() {
        Spi::run("CREATE TABLE test_highlighting AS SELECT * FROM generate_series(1, 10);");
        Spi::run("CREATE INDEX idxtest_highlighting ON test_highlighting USING zombodb ((test_highlighting.*));");
    }

    fn test_table(mut table: SpiTupleTable, expect: Vec<(&str, &str, i32, i64, i64)>) {
        let mut i = 0;
        while let Some(_) = table.next() {
            let token = table.get_datum::<&str>(2).unwrap();
            let ttype = table.get_datum::<&str>(3).unwrap();
            let pos = table.get_datum::<i32>(4).unwrap();
            let start_offset = table.get_datum::<i64>(5).unwrap();
            let end_offset = table.get_datum::<i64>(6).unwrap();

            let row_tuple = (ttype, token, pos, start_offset, end_offset);

            assert_eq!(expect[i], row_tuple);

            i += 1;
        }
        assert_eq!(expect.len(), i);
    }
}
