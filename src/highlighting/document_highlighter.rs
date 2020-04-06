use crate::elasticsearch::Elasticsearch;
use pgx::PgRelation;
use pgx::*;
use std::collections::HashMap;

#[derive(Debug)]
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

    pub fn highlight_wildcard(&self, token: &str) -> Option<&Vec<TokenEntry>> {
        unimplemented!()
    }

    pub fn highlight_regex(&self, token: &str) -> Option<&Vec<TokenEntry>> {
        unimplemented!()
    }

    // 'drinking green beer is better than drinking yellow beer which wine is worse than drinking yellow wine'
    //                                     ^^^^^^^^^^^^^^^                          ^^^^^^^^^^^^^^^
    // [ "drinking", "yellow" ]   query= drinking wo/1 yellow
    //
    // query= than w/2 wine
    // query= than wo/2 (wine or beer or cheese or food) w/5 cowbell
    pub fn highlight_phrase(&self, phrase: Vec<&str>) -> Option<Vec<TokenEntry>> {
        // 1)
        //
        //  for each token in phrase
        //
        //      highlight that token and remember all of its TokenEntries

        // 2)
        //
        //  ensure remembered token positions are somehow sequential and remember the
        //  matching TokenEntry for each token

        // 3)
        //
        //  return a vec of matching TokenEntries for each position

        unimplemented!()
    }
}

#[pg_extern]
fn highlight(
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
