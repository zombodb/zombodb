use crate::elasticsearch::analyze::*;
use crate::elasticsearch::Elasticsearch;
use pgx::PgRelation;
use pgx::*;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
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

    // todo: implement these two functions
    // pub fn highlight_wildcard(&self, token: &str) -> Option<&Vec<TokenEntry>> {
    //     unimplemented!()
    // }
    //
    // pub fn highlight_regex(&self, token: &str) -> Option<&Vec<TokenEntry>> {
    //     unimplemented!()
    // }

    // 'drinking green beer is better than drinking yellow beer which wine is worse than drinking yellow wine'
    //                                     ^^^^^^^^^^^^^^^                          ^^^^^^^^^^^^^^^
    // [ "drinking", "yellow" ]   query= drinking wo/1 yellow
    //
    // query= than w/2 wine
    // query= than wo/2 (wine or beer or cheese or food) w/5 cowbell
    pub fn highlight_phrase(
        &self,
        index: PgRelation,
        field: &str,
        phrase_str: &str,
    ) -> Option<Vec<(String, &TokenEntry)>> {
        if phrase_str.is_empty() {
            return None;
        }

        //0) change phrase from a Vec<&str> to a &str and parse to a Vec<&str>
        let phrase = analyze_with_field(index, field, phrase_str)
            .map(|parts| parts.1)
            .collect::<Vec<String>>();

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

        //make run off the elasticsearch analyze
        //bug: does not work with one word:

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

        let mut filtered_pool = HashMap::<&str, Vec<&TokenEntry>>::new();
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
                    if token_entry_one.position + 1 == token_entry_two.position {
                        let entry_list = filtered_pool.entry(first).or_insert_with(|| Vec::new());
                        if !entry_list.contains(&token_entry_one) {
                            entry_list.push(token_entry_one);
                        }

                        let entry_list = filtered_pool.entry(*second).or_insert_with(|| Vec::new());
                        if !entry_list.contains(&token_entry_two) {
                            entry_list.push(token_entry_two);
                        }
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

#[pg_extern]
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

#[pg_extern]
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
