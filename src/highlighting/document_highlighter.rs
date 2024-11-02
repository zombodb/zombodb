use crate::elasticsearch::Elasticsearch;
use crate::highlighting::analyze::AnalyzedToken;
use crate::utils::{get_highlight_analysis_info, has_date_subfield};
use crate::zql::ast::{IndexLink, ProximityPart, ProximityTerm, QualifiedField, Term};
use levenshtein::*;
use pgrx::pg_sys::{AsPgCStr, InvalidOid};
use pgrx::prelude::*;
use pgrx::{direct_function_call, PgRelation};
use regex::Regex;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Deref;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct TokenEntry {
    pub type_: Cow<'static, str>,
    pub array_index: u32,
    pub position: u32,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug)]
enum DataType {
    String,
    Integer,
    Float,
    Date,
    Bool,
}

#[derive(Debug)]
pub struct DocumentHighlighter<'a> {
    lookup: BTreeMap<Cow<'a, str>, Vec<TokenEntry>>,
    data_type: Option<DataType>,
    field: String,
    index_oid: pg_sys::Oid,
}

#[rustfmt::skip]
fn make_date(s: &str) -> Option<TimestampWithTimeZone> {
    unsafe {
        let date_str = s.as_pg_cstr();
        let date: Option<TimestampWithTimeZone> = PgTryBuilder::new(|| {
            direct_function_call(
                pg_sys::timestamptz_in,
                &[
                    Some(pg_sys::Datum::from(date_str)),
                    InvalidOid.into_datum(),
                    (-1i32).into_datum(),
                ],
            )
        })
        .catch_when(PgSqlErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW, |_| None)
        .catch_when(PgSqlErrorCode::ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE, |_| None)
        .catch_when(PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT, |_| None)
        .execute();
        pg_sys::pfree(date_str.cast());
        
        date
    }
}

macro_rules! compare_date {
    ($left:tt, $cmp:tt, $right:tt) => {{
        let token = make_date($left);
        let term = make_date($right);
        if token.is_none() || term.is_none() {
            return false;
        }
        let token: i64 = token.unwrap().into();
        let term: i64 = term.unwrap().into();

        token.$cmp(&term)
    }};
}

macro_rules! compare_float {
    ($left:tt, $cmp:tt, $right:tt) => {{
        let token = f64::from_str($left);
        let term = f64::from_str($right);
        if token.is_err() || term.is_err() {
            return false;
        }

        token.unwrap().$cmp(&term.unwrap())
    }};
}

macro_rules! compare_integer {
    ($left:tt, $cmp:tt, $right:tt) => {{
        let token = i64::from_str($left);
        if token.is_err() {
            return false;
        }

        let term = match i64::from_str($right) {
            Ok(i) => i,
            Err(_) => match f64::from_str($right) {
                Ok(f) => f as i64,
                Err(_) => {
                    return false;
                }
            },
        };

        token.unwrap().$cmp(&term)
    }};
}

pub type HighlightMatches<'a> = Vec<(&'a Cow<'a, str>, &'a TokenEntry)>;
pub type HighlightCollection<'a> = Vec<((QualifiedField, String), HighlightMatches<'a>)>;

impl<'a> DocumentHighlighter<'a> {
    pub fn new(index: &PgRelation, field: &str, value: &'a Value) -> Self {
        if let Value::String(_) = value {
            let (field_type, _, index_analyzer) = get_highlight_analysis_info(index, field);
            let is_date = has_date_subfield(index, field);
            DocumentHighlighter::from_json(
                index,
                field,
                value,
                0,
                &field_type,
                is_date,
                &index_analyzer,
            )
            .into_iter()
            .next()
            .unwrap()
            .1
        } else {
            panic!("supplied `value` is not a Value::String")
        }
    }

    pub fn from_json(
        index: &PgRelation,
        field: &str,
        value: &'a Value,
        array_index: u32,
        field_type: &Option<String>,
        is_date: bool,
        index_analyzer: &Option<String>,
    ) -> HashMap<(String, u32), Self> {
        match value {
            Value::Object(o) => {
                // recursively build highlighters for each (k, v) pair
                // in this JSON Object
                let mut highlighters = HashMap::new();

                o.iter().for_each(|(k, v)| {
                    highlighters.extend(DocumentHighlighter::from_json(
                        index,
                        &format!("{}.{}", field, k),
                        v,
                        array_index,
                        field_type,
                        is_date,
                        index_analyzer,
                    ))
                });

                highlighters
            }

            Value::Array(a) => {
                // recursively build highlighters for each element
                // in this JSON Array
                let mut highlighters = HashMap::new();

                a.iter().enumerate().for_each(|(i, v)| {
                    highlighters.extend(DocumentHighlighter::from_json(
                        index,
                        field,
                        v,
                        array_index + i as u32,
                        field_type,
                        is_date,
                        index_analyzer,
                    ))
                });

                highlighters
            }

            _ => {
                let mut result = DocumentHighlighter {
                    lookup: Default::default(),
                    data_type: None,
                    field: field.into(),
                    index_oid: index.oid(),
                };

                result.analyze_document(
                    index,
                    field,
                    value,
                    array_index,
                    field_type,
                    is_date,
                    index_analyzer,
                );

                let mut highlighters = HashMap::new();
                highlighters.insert((field.into(), array_index), result);
                highlighters
            }
        }
    }

    fn analyze_document(
        &mut self,
        index: &PgRelation,
        field: &str,
        value: &'a Value,
        array_index: u32,
        field_type: &Option<String>,
        is_date: bool,
        analyzer: &Option<String>,
    ) {
        match value {
            Value::Bool(b) => {
                self.lookup
                    .entry(Cow::Owned(b.to_string()))
                    .or_default()
                    .push(TokenEntry {
                        type_: "<BOOLEAN>".into(),
                        array_index,
                        position: 1,
                        start_offset: 0,
                        end_offset: 0,
                    });
                self.data_type = Some(DataType::Bool);
            }

            Value::Number(number) => {
                if number.is_f64() {
                    self.lookup
                        .entry(Cow::Owned(number.as_f64().unwrap().to_string()))
                        .or_default()
                        .push(TokenEntry {
                            type_: "<FLOAT>".into(),
                            array_index,
                            position: 1,
                            start_offset: 0,
                            end_offset: 0,
                        });
                    self.data_type = Some(DataType::Float);
                } else if number.is_u64() {
                    self.lookup
                        .entry(Cow::Owned(number.as_u64().unwrap().to_string()))
                        .or_default()
                        .push(TokenEntry {
                            type_: "<UNSIGNED_INT>".into(),
                            array_index,
                            position: 1,
                            start_offset: 0,
                            end_offset: 0,
                        });
                    self.data_type = Some(DataType::Integer);
                } else {
                    self.lookup
                        .entry(Cow::Owned(number.as_i64().unwrap().to_string()))
                        .or_default()
                        .push(TokenEntry {
                            type_: "<SIGNED_INT>".into(),
                            array_index,
                            position: 1,
                            start_offset: 0,
                            end_offset: 0,
                        });
                    self.data_type = Some(DataType::Integer);
                }
            }

            Value::String(s) if is_date => {
                self.lookup
                    .entry(Cow::Borrowed(s))
                    .or_default()
                    .push(TokenEntry {
                        type_: "<DATE>".into(),
                        array_index,
                        position: 1,
                        start_offset: 0,
                        end_offset: 0,
                    });
                self.data_type = Some(DataType::Date);
            }

            Value::String(s) => {
                #[inline]
                fn map_analyzed_token(
                    array_index: u32,
                    token: AnalyzedToken,
                ) -> (Cow<str>, TokenEntry) {
                    (
                        token.token,
                        TokenEntry {
                            type_: Cow::Borrowed(token.type_),
                            array_index,
                            position: (token.position + 1) as u32,
                            start_offset: token.start as u64,
                            end_offset: token.end as u64,
                        },
                    )
                }

                let results: Box<dyn Iterator<Item = (Cow<str>, TokenEntry)>> =
                    match (analyzer, field_type) {
                        (None, None) => Box::new(
                            crate::highlighting::analyze::standard(s)
                                .map(|x| map_analyzed_token(array_index, x)),
                        ),

                        (Some(analyzer), _)
                            if analyzer == "standard"
                                || analyzer == "zdb_standard"
                                || analyzer == "fulltext" =>
                        {
                            Box::new(
                                crate::highlighting::analyze::standard(s)
                                    .map(|x| map_analyzed_token(array_index, x)),
                            )
                        }
                        (Some(analyzer), _) if analyzer == "fulltext_with_shingles" => Box::new(
                            crate::highlighting::analyze::fulltext_with_shingles(s)
                                .map(|x| map_analyzed_token(array_index, x)),
                        ),
                        (_, Some(field_type)) if field_type == "keyword" => {
                            let lcase = s.to_lowercase();
                            let len = lcase.len();
                            Box::new(std::iter::once((
                                Cow::Owned(lcase),
                                TokenEntry {
                                    type_: "word".into(),
                                    array_index,
                                    position: 1,
                                    start_offset: 0,
                                    end_offset: len.try_into().unwrap(),
                                },
                            )))
                        }
                        _ => {
                            let es = Elasticsearch::new(index);
                            Box::new(
                                es.analyze_with_field(field, s)
                                    .execute()
                                    .expect("failed to analyze text for highlighting")
                                    .tokens
                                    .into_iter()
                                    .map(|token| {
                                        (
                                            Cow::Owned(token.token),
                                            TokenEntry {
                                                type_: Cow::Owned(token.type_),
                                                array_index,
                                                position: (<i32 as TryInto<u32>>::try_into(
                                                    token.position,
                                                )
                                                .expect("position overflowed"))
                                                    + 1,
                                                start_offset: token
                                                    .start_offset
                                                    .try_into()
                                                    .expect("start_offset overflowed"),
                                                end_offset: token
                                                    .end_offset
                                                    .try_into()
                                                    .expect("end_offset overflowed"),
                                            },
                                        )
                                    }),
                            )
                        }
                    };

                for (token, entry) in results {
                    pgrx::check_for_interrupts!();
                    self.lookup.entry(token).or_default().push(entry);
                }
                self.data_type = Some(DataType::String);
            }

            Value::Array(values) => values.iter().enumerate().for_each(|(idx, v)| {
                self.analyze_document(
                    index,
                    field,
                    v,
                    array_index + (idx as u32),
                    field_type,
                    is_date,
                    analyzer,
                )
            }),

            Value::Object(o) => {
                o.iter().for_each(|(k, v)| {
                    self.analyze_document(
                        index,
                        &format!("{}.{}", field, k),
                        v,
                        array_index,
                        field_type,
                        is_date,
                        analyzer,
                    );
                });
            }

            Value::Null => { /* noop */ }
        }
    }

    pub fn gt_func(&self) -> fn(&str, &str) -> bool {
        match &self.data_type {
            Some(DataType::String) => str::gt,
            Some(DataType::Float) => |token: &str, term: &str| compare_float!(token, gt, term),
            Some(DataType::Integer) => |token: &str, term: &str| compare_integer!(token, gt, term),
            Some(DataType::Date) => |token: &str, term: &str| compare_date!(token, gt, term),
            Some(DataType::Bool) => |_, _| false,
            None => |_, _| false,
        }
    }

    pub fn lt_func(&self) -> fn(&str, &str) -> bool {
        match &self.data_type {
            Some(DataType::String) => str::lt,
            Some(DataType::Float) => |token: &str, term: &str| compare_float!(token, lt, term),
            Some(DataType::Integer) => |token: &str, term: &str| compare_integer!(token, lt, term),
            Some(DataType::Date) => |token: &str, term: &str| compare_date!(token, lt, term),
            Some(DataType::Bool) => |_, _| false,
            None => |_, _| false,
        }
    }

    pub fn ge_func(&self) -> fn(&str, &str) -> bool {
        match &self.data_type {
            Some(DataType::String) => str::ge,
            Some(DataType::Float) => |token: &str, term: &str| compare_float!(token, ge, term),
            Some(DataType::Integer) => |token: &str, term: &str| compare_integer!(token, ge, term),
            Some(DataType::Date) => |token: &str, term: &str| compare_date!(token, ge, term),
            Some(DataType::Bool) => |token: &str, term: &str| token == term,
            None => |_, _| false,
        }
    }

    pub fn le_func(&self) -> fn(&str, &str) -> bool {
        match &self.data_type {
            Some(DataType::String) => str::le,
            Some(DataType::Float) => |token: &str, term: &str| compare_float!(token, le, term),
            Some(DataType::Integer) => |token: &str, term: &str| compare_integer!(token, le, term),
            Some(DataType::Date) => |token: &str, term: &str| compare_date!(token, le, term),
            Some(DataType::Bool) => |token: &str, term: &str| token == term,
            None => |_, _| false,
        }
    }

    pub fn highlight_term(&'a self, term: &Term) -> Option<HighlightMatches<'a>> {
        match term {
            Term::MatchAll => None,
            Term::MatchNone => None,
            Term::String(s, _) => self.highlight_token(s),
            Term::Prefix(s, _) => self.highlight_wildcard(s),
            Term::PhrasePrefix(_, _) => unimplemented!("prefix phrases cannot be highlighted"),
            Term::Phrase(s, _) | Term::PhraseWithWildcard(s, _) => {
                self.highlight_phrase(&self.field, s)
            }
            Term::Wildcard(w, _) => self.highlight_wildcard(w),
            Term::Regex(r, _) => self.highlight_regex(r),
            Term::Fuzzy(f, p, _) => self.highlight_fuzzy(f, *p),
            Term::Range(_, _, _) => unimplemented!(),
            Term::Null => self.highlight_null(),
            Term::ParsedArray(_, _) => unimplemented!(),
            Term::UnparsedArray(_, _) => unimplemented!(),
            Term::ProximityChain(v) => self.highlight_proximity(v),
        }
    }

    pub fn highlight_null(&'a self) -> Option<HighlightMatches<'a>> {
        if self.lookup.is_empty() {
            Some(vec![])
        } else {
            None
        }
    }

    pub fn highlight_token(&'a self, token: &str) -> Option<HighlightMatches<'a>> {
        let token = Cow::Owned(token.to_lowercase());
        let token_entries_vec = self.lookup.get_key_value(&token);
        token_entries_vec.map(|(token, vec)| vec.iter().map(|e| (token, e)).collect())
    }

    pub fn highlight_token_scan<F: Fn(&str, &str) -> bool>(
        &'a self,
        term: &str,
        eval: F,
    ) -> Option<HighlightMatches<'a>> {
        let term = term.to_lowercase();
        let mut result = Vec::new();
        for (token, entries) in &self.lookup {
            if eval(token, &term) {
                for entry in entries {
                    result.push((token, entry))
                }
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn highlight_wildcard(&'a self, token: &str) -> Option<HighlightMatches<'a>> {
        let token = token.to_lowercase();
        let _char_looking_for_asterisk = '*';
        let _char_looking_for_question = '?';
        let mut new_regex = String::from("^");
        for char in token.chars() {
            if char == _char_looking_for_question {
                new_regex.push('.')
            } else if char == _char_looking_for_asterisk {
                new_regex.push('.');
                new_regex.push(char);
            } else if char == '$' {
                new_regex.push('\\');
                new_regex.push(char);
            } else {
                new_regex.push(char);
            }
        }
        new_regex.push('$');
        self.highlight_regex(new_regex.deref())
    }

    pub fn highlight_regex(&'a self, regex: &str) -> Option<HighlightMatches<'a>> {
        let regex = Regex::new(regex).unwrap();
        let mut result = Vec::new();
        for (key, token_entries) in self.lookup.iter() {
            if regex.is_match(key) {
                for token_entry in token_entries {
                    result.push((key, token_entry));
                }
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn highlight_fuzzy(&'a self, fuzzy_key: &str, prefix: u8) -> Option<HighlightMatches<'a>> {
        let fuzzy_key = fuzzy_key.to_lowercase();
        let mut result = Vec::new();
        let fuzzy_low = 3;
        let fuzzy_high = 6;
        if prefix >= fuzzy_key.len() as u8 {
            return self.highlight_token(&fuzzy_key);
        }
        let prefix_string = &fuzzy_key[0..prefix as usize];
        for (token, token_entries) in self.lookup.iter() {
            if token.starts_with(prefix_string) {
                if fuzzy_key.len() < fuzzy_low {
                    if levenshtein(token, &fuzzy_key) as i32 == 0 {
                        for token_entry in token_entries {
                            result.push((token, token_entry));
                        }
                    }
                } else if fuzzy_key.len() >= fuzzy_low && fuzzy_key.len() < fuzzy_high {
                    if levenshtein(token, &fuzzy_key) as i32 <= 1 {
                        for token_entry in token_entries {
                            result.push((token, token_entry));
                        }
                    }
                } else if levenshtein(token, &fuzzy_key) as i32 <= 2 {
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
        &'a self,
        field: &str,
        phrase_str: &str,
    ) -> Option<HighlightMatches<'a>> {
        if phrase_str.is_empty() {
            return None;
        }

        let phrase_str = phrase_str.to_lowercase();
        let prox_term = ProximityTerm::make_proximity_chain(
            &QualifiedField {
                index: Some(IndexLink::from_relation(unsafe {
                    &PgRelation::open(self.index_oid)
                })),
                field: field.to_string(),
            },
            &phrase_str,
            None,
        );
        let term = prox_term.to_term();
        self.highlight_term(&term)
    }

    // 'drinking green beer is better than drinking yellow beer which wine is worse than drinking yellow wine'
    //                                     ^^^^^^^^^^^^^^^                          ^^^^^^^^^^^^^^^
    // [ "drinking", "yellow" ]   query= drinking wo/1 yellow
    //
    // query= than w/2 wine
    // query= than wo/2 (wine or beer or cheese or food) w/5 cowbell
    pub fn highlight_proximity(&'a self, phrase: &[ProximityPart]) -> Option<HighlightMatches<'a>> {
        if phrase.is_empty() {
            return None;
        }

        let first_part = phrase.first().unwrap();
        let mut final_matches = Vec::new();

        let phrase2 = phrase
            .iter()
            .map(|p| {
                (
                    p,
                    ProximityTerm::to_terms(&p.words)
                        .into_iter()
                        .filter_map(|t| {
                            self.highlight_term(&t).map(|highlights| {
                                (highlights, matches!(t, Term::ProximityChain(_)))
                            })
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        for word in &first_part.words {
            let first_word_entries = match self.highlight_term(&word.to_term()) {
                None => continue,
                Some(first_word_entries) if phrase.len() == 1 => return Some(first_word_entries),
                Some(first_word_entries) => first_word_entries,
            };

            for (token, token_entry) in first_word_entries {
                let array_index = token_entry.array_index;
                let mut start = vec![token_entry.position]; // 0
                let mut possibilities = Vec::new();
                let mut is_valid = true;

                possibilities.push((token, token_entry));

                let mut iter = phrase2.iter().peekable();
                while let Some((current, _matches)) = iter.next() {
                    let next = match iter.peek() {
                        Some(next) => next,
                        None => break,
                    };

                    let distance = current.distance.as_ref().map_or(0, |v| v.distance);
                    let order = current.distance.as_ref().map_or(false, |v| v.in_order);
                    match self.look_for_match_ex(&next.1, distance, order, start, array_index) {
                        None => {
                            is_valid = false;
                            break;
                        }
                        Some(next_entries) => {
                            start = next_entries
                                .iter()
                                .map(|e| e.1.position)
                                .collect::<Vec<u32>>();
                            possibilities.extend(next_entries);
                        }
                    }
                }

                if is_valid {
                    final_matches.extend(possibilities);
                }
            }
        }

        if final_matches.is_empty() {
            None
        } else {
            Some(final_matches)
        }
    }

    #[allow(dead_code)]
    fn look_for_match(
        &'a self,
        words: &[Term],
        distance: u32,
        order: bool,
        starting_point: Vec<u32>,
        array_index: u32,
    ) -> Option<HighlightMatches<'a>> {
        let words = words
            .iter()
            .filter_map(|t| {
                self.highlight_term(t)
                    .map(|highlights| (highlights, matches!(t, Term::ProximityChain(_))))
            })
            .collect::<Vec<_>>();
        self.look_for_match_ex(&words, distance, order, starting_point, array_index)
    }

    fn look_for_match_ex(
        &'a self,
        words: &[(HighlightMatches<'a>, bool)],
        distance: u32,
        order: bool,
        starting_point: Vec<u32>,
        _array_index: u32, // NB:  Pretty sure we don't need to care about the array_index for proximity clauses
    ) -> Option<HighlightMatches<'a>> {
        let mut matches = Vec::new();
        for (highlights, is_prox_chain) in words {
            let mut highlights = highlights
                .iter()
                .map(|(cow, entry)| (*cow, *entry))
                .collect::<HighlightMatches>();
            highlights.sort_by_key(|(_, entry)| entry.position);
            for start in &starting_point {
                let start = *start;
                let range = if order {
                    start + 1..=start + 1 + distance
                } else {
                    (start as i64 - 1 - distance as i64).max(1) as u32..=start + 1 + distance
                };

                for point in range {
                    if point != start {
                        while let Ok(idx) =
                            highlights.binary_search_by(|(_, entry)| entry.position.cmp(&point))
                        {
                            if *is_prox_chain {
                                matches.append(&mut highlights);
                            } else {
                                let e = highlights.get(idx).unwrap();
                                matches.push(*e);
                            }
                            if !highlights.is_empty() {
                                highlights.remove(idx);
                            }
                        }
                    }
                }
            }
        }

        if matches.is_empty() {
            None
        } else {
            Some(matches)
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
fn highlight_term(
    index: PgRelation,
    field_name: &str,
    text: String,
    token_to_highlight: String,
) -> TableIterator<
    'static,
    (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let value = serde_json::Value::String(text);
    let highlighter = DocumentHighlighter::new(&index, field_name, &value);
    let highlights = highlighter.highlight_token(&token_to_highlight);

    TableIterator::new(match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.to_owned(),
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
    })
}

#[pg_extern(immutable, parallel_safe)]
fn highlight_phrase(
    index: PgRelation,
    field_name: &str,
    text: String,
    tokens_to_highlight: &str,
) -> TableIterator<
    'static,
    (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let value = serde_json::Value::String(text);
    let highlighter = DocumentHighlighter::new(&index, field_name, &value);
    let highlights = highlighter.highlight_phrase(field_name, tokens_to_highlight);

    TableIterator::new(match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.to_owned(),
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
    })
}

#[pg_extern(immutable, parallel_safe)]
fn highlight_wildcard(
    index: PgRelation,
    field_name: &str,
    text: String,
    token_to_highlight: &str,
) -> TableIterator<
    'static,
    (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let value = serde_json::Value::String(text);
    let highlighter = DocumentHighlighter::new(&index, field_name, &value);
    let highlights = highlighter.highlight_wildcard(token_to_highlight);

    TableIterator::new(match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.to_owned(),
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
    })
}

#[pg_extern(immutable, parallel_safe)]
fn highlight_regex(
    index: PgRelation,
    field_name: &str,
    text: String,
    token_to_highlight: &str,
) -> TableIterator<
    'static,
    (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let value = serde_json::Value::String(text);
    let highlighter = DocumentHighlighter::new(&index, field_name, &value);
    let highlights = highlighter.highlight_regex(token_to_highlight);

    TableIterator::new(match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.to_owned(),
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
    })
}

#[pg_extern(immutable, parallel_safe)]
fn highlight_fuzzy(
    index: PgRelation,
    field_name: &str,
    text: String,
    token_to_highlight: &str,
    prefix: i32,
) -> TableIterator<
    'static,
    (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    if prefix < 0 {
        panic!("negative prefixes not allowed");
    }
    let value = serde_json::Value::String(text);
    let highlighter = DocumentHighlighter::new(&index, field_name, &value);
    let highlights = highlighter.highlight_fuzzy(token_to_highlight, prefix as u8);

    TableIterator::new(match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.to_owned(),
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
    })
}

//  select zdb.highlight_proximity('idx_test','test','this is a test', ARRAY['{"word": "this", distance:2, in_order: false}'::proximitypart, '{"word": "test", distance: 0, in_order: false}'::proximitypart]);
#[pg_extern(immutable, parallel_safe)]
fn highlight_proximity(
    index: PgRelation,
    field_name: &str,
    text: String,
    prox_clause: Vec<Option<ProximityPart>>,
    dedup_results: default!(bool, true),
) -> TableIterator<
    'static,
    (
        name!(field_name, String),
        name!(term, String),
        name!(type, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let prox_clause = prox_clause
        .into_iter()
        .map(|e| e.unwrap())
        .collect::<Vec<ProximityPart>>();
    let value = serde_json::Value::String(text);
    let highlighter = DocumentHighlighter::new(&index, field_name, &value);
    let highlights = highlighter.highlight_proximity(&prox_clause);

    let iter = match highlights {
        Some(vec) => vec
            .iter()
            .map(|e| {
                (
                    field_name.to_owned(),
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
    };

    let iter: Box<dyn Iterator<Item = _>> = if dedup_results {
        use std::collections::HashSet;
        Box::new(iter.collect::<HashSet<_>>().into_iter())
    } else {
        Box::new(iter)
    };
    TableIterator::new(iter)
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::highlighting::document_highlighter::{DocumentHighlighter, TokenEntry};
    use crate::zql::ast::Term;
    use pgrx::spi::SpiTupleTable;
    use pgrx::*;
    use regex::Regex;
    use serde_json::*;
    use std::borrow::Cow;

    #[pg_test(error = "no matches found")]
    #[initialize(es = true)]
    fn test_look_for_match_none() {
        let title = "look_for_match_none";
        start_table_and_index(title);

        let index = unsafe {
            PgRelation::open_with_name("idxtest_highlighting_look_for_match_none").unwrap()
        };
        let value = serde_json::Value::String("this is a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("test", None)], 1, true, vec![0], 0)
            .expect("no matches found");
        matches.is_empty();
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_look_for_match_in_order_one() {
        let title = "look_for_match_in_order";
        start_table_and_index(title);

        let index = unsafe {
            PgRelation::open_with_name("idxtest_highlighting_look_for_match_in_order").unwrap()
        };
        let value = serde_json::Value::String("this is a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("is", None)], 0, true, vec![1], 0)
            .expect("no matches found");
        let mut expected = Vec::new();
        let value = (
            &Cow::Borrowed("is"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 2,
                start_offset: 5,
                end_offset: 7,
            },
        );
        expected.push(value);
        assert_eq!(matches, expected)
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_look_for_match_out_of_order_one() {
        let title = "look_for_match_out_of_order";
        start_table_and_index(title);
        let index = unsafe {
            PgRelation::open_with_name("idxtest_highlighting_look_for_match_out_of_order").unwrap()
        };
        let value = serde_json::Value::String("this is a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("this", None)], 0, false, vec![2], 0)
            .expect("no matches found");
        let mut expected = Vec::new();
        let value_one = (
            &Cow::Borrowed("this"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 1,
                start_offset: 0,
                end_offset: 4,
            },
        );
        expected.push(value_one);
        assert_eq!(matches, expected)
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_look_for_match_in_order_two() {
        let title = "look_for_match_out_of_order_two";
        start_table_and_index(title);
        let index = unsafe {
            PgRelation::open_with_name("idxtest_highlighting_look_for_match_out_of_order_two")
                .unwrap()
        };
        let value = serde_json::Value::String("this is a test and this is also a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("is", None)], 0, false, vec![1, 6], 0)
            .expect("no matches found");
        let mut expected = Vec::new();
        let value_one = (
            &Cow::Borrowed("is"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 2,
                start_offset: 5,
                end_offset: 7,
            },
        );
        let value_two = (
            &Cow::Borrowed("is"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 7,
                start_offset: 24,
                end_offset: 26,
            },
        );
        expected.push(value_one);
        expected.push(value_two);
        assert_eq!(matches, expected)
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_look_for_match_out_of_order_two() {
        let title = "look_for_match_out_of_order_two";
        start_table_and_index(title);
        let index = unsafe {
            PgRelation::open_with_name("idxtest_highlighting_look_for_match_out_of_order_two")
                .unwrap()
        };
        let value = serde_json::Value::String("this is a test and this is also a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("this", None)], 0, false, vec![2, 7], 0)
            .expect("no matches found");
        let mut expected = Vec::new();
        let value_one = (
            &Cow::Borrowed("this"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 1,
                start_offset: 0,
                end_offset: 4,
            },
        );
        let value_two = (
            &Cow::Borrowed("this"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 6,
                start_offset: 19,
                end_offset: 23,
            },
        );
        expected.push(value_one);
        expected.push(value_two);
        assert_eq!(matches, expected)
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_look_for_match_in_order_two_different_dist() {
        let title = "look_for_match_in_order_two_diff_dist";
        start_table_and_index(title);
        let index = unsafe {
            PgRelation::open_with_name("idxtest_highlighting_look_for_match_in_order_two_diff_dist")
                .unwrap()
        };
        let value = serde_json::Value::String("this is a test and this is also a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("test", None)], 3, true, vec![1, 6], 0)
            .expect("no matches found");
        let mut expected = Vec::new();
        let value_one = (
            &Cow::Borrowed("test"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 4,
                start_offset: 10,
                end_offset: 14,
            },
        );
        let value_two = (
            &Cow::Borrowed("test"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 10,
                start_offset: 34,
                end_offset: 38,
            },
        );
        expected.push(value_one);
        expected.push(value_two);
        assert_eq!(matches, expected)
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_look_for_match_out_of_order_two_diff_dist() {
        let title = "look_for_match_out_of_order_two_diff_dist";
        start_table_and_index(title);
        let index = unsafe {
            PgRelation::open_with_name(
                "idxtest_highlighting_look_for_match_out_of_order_two_diff_dist",
            )
            .unwrap()
        };
        let value = serde_json::Value::String("this is a test and this is also a test".into());
        let dh = DocumentHighlighter::new(&index, "test_field", &value);

        let matches = dh
            .look_for_match(&[Term::String("this", None)], 3, false, vec![3, 9], 0)
            .expect("no matches found");
        let mut expected = Vec::new();
        let value_one = (
            &Cow::Borrowed("this"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 1,
                start_offset: 0,
                end_offset: 4,
            },
        );
        let value_two = (
            &Cow::Borrowed("this"),
            &TokenEntry {
                type_: "<ALPHANUM>".into(),
                array_index: 0,
                position: 6,
                start_offset: 19,
                end_offset: 23,
            },
        );
        expected.push(value_one);
        expected.push(value_two);
        assert_eq!(matches, expected)
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_term() -> spi::Result<()> {
        let title = "term";
        start_table_and_index(title);
        let select: String = format!(
            "select * from zdb.highlight_term('idxtest_highlighting_{}', 'test_field', 'it is a test and it is a good one', 'it') order by position;", title
        );
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // name       | it   | <ALPHANUM> |        1 |            0 |          2
            // name       | it   | <ALPHANUM> |        6 |           17 |         19
            let expect = vec![
                ("<ALPHANUM>", "it", 1, 0, 2),
                ("<ALPHANUM>", "it", 6, 17, 19),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_phrase() -> spi::Result<()> {
        let title = "phrase";
        start_table_and_index(title);
        let select: String = format!("select * from zdb.highlight_phrase('idxtest_highlighting_{}', 'test_field', 'it is a test and it is a good one', 'it is a') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | it   | <ALPHANUM> |        1 |            0 |          2
            // test_field | is   | <ALPHANUM> |        2 |            3 |          5
            // test_field | a    | <ALPHANUM> |        3 |            6 |          7
            // test_field | it   | <ALPHANUM> |        6 |           17 |         19
            // test_field | is   | <ALPHANUM> |        7 |           20 |         22
            // test_field | a    | <ALPHANUM> |        8 |           23 |         24
            let expect = vec![
                ("<ALPHANUM>", "it", 1, 0, 2),
                ("<ALPHANUM>", "is", 2, 3, 5),
                ("<ALPHANUM>", "a", 3, 6, 7),
                ("<ALPHANUM>", "it", 6, 17, 19),
                ("<ALPHANUM>", "is", 7, 20, 22),
                ("<ALPHANUM>", "a", 8, 23, 24),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_phrase_as_one_word() -> spi::Result<()> {
        let title = "phrase_one_word";
        start_table_and_index(title);
        let select: String = format!("select * from zdb.highlight_phrase('idxtest_highlighting_{}', 'test_field', 'it is a test and it is a good one', 'it') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | it   | <ALPHANUM> |        1 |            0 |          2
            // test_field | it   | <ALPHANUM> |        6 |           17 |         19
            let expect = vec![
                ("<ALPHANUM>", "it", 1, 0, 2),
                ("<ALPHANUM>", "it", 6, 17, 19),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_phrase_with_phrase_not_in_text() -> spi::Result<()> {
        let title = "phrase_not_in_text";
        start_table_and_index(title);
        let select: String = format!("select * from zdb.highlight_phrase('idxtest_highlighting_{}', 'test_field', 'it is a test and it is a good one', 'banana') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            let expect = vec![];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_wildcard_with_asterisk() -> spi::Result<()> {
        let title = "wildcard_ast";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_wildcard('idxtest_highlighting_{}', 'test_field', 'Mom landed a man on the moon', 'm*n') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            //  test_field | man  | <ALPHANUM> |        4 |           13 |         16
            //  test_field | moon | <ALPHANUM> |        7 |           24 |         28
            let expect = vec![
                ("<ALPHANUM>", "man", 4, 13, 16),
                ("<ALPHANUM>", "moon", 7, 24, 28),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_wildcard_with_question_mark() -> spi::Result<()> {
        let title = "wildcard_question";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_wildcard('idxtest_highlighting_{}', 'test_field', 'Mom landed a man on the moon', 'm?n') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            //  test_field | man  | <ALPHANUM> |        4 |           13 |         16
            let expect = vec![("<ALPHANUM>", "man", 4, 13, 16)];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_wildcard_with_no_match() -> spi::Result<()> {
        let title = "wildcard_no_match";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_wildcard('idxtest_highlighting_{}', 'test_field', 'Mom landed a man on the moon', 'n*n') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            let expect = vec![];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_regex() -> spi::Result<()> {
        let title = "regex";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_regex('idxtest_highlighting_{}', 'test_field', 'Mom landed a man on the moon', '^m.*$') order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | mom  | <ALPHANUM> |        1 |            0 |          3
            // test_field | man  | <ALPHANUM> |        4 |           13 |         16
            // test_field | moon | <ALPHANUM> |        7 |           24 |         28
            let expect = vec![
                ("<ALPHANUM>", "mom", 1, 0, 3),
                ("<ALPHANUM>", "man", 4, 13, 16),
                ("<ALPHANUM>", "moon", 7, 24, 28),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_regex_test_two() -> spi::Result<()> {
        let title = "regex_test_two";
        start_table_and_index(title);
        let regex_ex = Regex::new("^m.?$").unwrap();
        let select = format!("select * from zdb.highlight_regex('idxtest_highlighting_{}', 'test_field', 'Mom landed a man on the moon', '{}') order by position;", title, regex_ex.as_str());
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            let expect = vec![];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_fuzzy_correct_three_char_term() -> spi::Result<()> {
        let title = "fuzzy_three";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_fuzzy('idxtest_highlighting_{}', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt c', 'cot', 1) order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field  | colt | <ALPHANUM> |        2 |            0 |          4
            // test_field  | cot  | <ALPHANUM> |        3 |            5 |          9
            // test_field  | co   | <ALPHANUM> |        6 |           10 |         13
            // test_field  | cat  | <ALPHANUM> |        9 |           26 |         28
            let expect = vec![
                ("<ALPHANUM>", "colt", 2, 5, 9),
                ("<ALPHANUM>", "cot", 3, 10, 13),
                ("<ALPHANUM>", "co", 6, 26, 28),
                ("<ALPHANUM>", "cat", 9, 41, 44),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_fuzzy_correct_two_char_string() -> spi::Result<()> {
        let title = "fuzzy_two";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_fuzzy('idxtest_highlighting_{}', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt c', 'co', 1) order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name  | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | co   | <ALPHANUM> |        6 |           26 |         28
            let expect = vec![("<ALPHANUM>", "co", 6, 26, 28)];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_fuzzy_6_char_string() -> spi::Result<()> {
        let title = "fuzzy_six";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_fuzzy('idxtest_highlighting_{}', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt c cott cooler', 'colter', 2) order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term   |    type    | position | start_offset | end_offset
            // -----------+--------+------------+----------+--------------+------------
            // test_field | colt   | <ALPHANUM> |        2 |            5 |          9
            // test_field | colter | <ALPHANUM> |        8 |           34 |         40
            // test_field | cooler | <ALPHANUM> |       13 |           57 |         63

            let expect = vec![
                ("<ALPHANUM>", "colt", 2, 5, 9),
                ("<ALPHANUM>", "colter", 8, 34, 40),
                ("<ALPHANUM>", "cooler", 13, 57, 63),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    // fn test_highlighter_fuzzy_with_prefix_number_longer_then_given_string() {
    fn test_highlighter_fuzzy_1() -> spi::Result<()> {
        let title = "fuzzy_long_prefix";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_fuzzy('idxtest_highlighting_{}', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt', 'cot', 4) order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | cot  | <ALPHANUM> |        3 |           10 |         13

            let expect = vec![("<ALPHANUM>", "cot", 3, 10, 13)];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    // fn test_highlighter_fuzzy_with_prefix_number_longer_then_given_string_with_non_return() {
    fn test_highlighter_fuzzy_2() -> spi::Result<()> {
        let title = "fuzzy_long_prefix_no_return";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_fuzzy('idxtest_highlighting_{}', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt', 'cet', 4) order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------

            let expect = vec![];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test(error = "negative prefixes not allowed")]
    #[initialize(es = true)]
    fn test_highlighter_fuzzy_with_negative_prefix() -> spi::Result<()> {
        let title = "fuzzy_neg_prefix";
        start_table_and_index(title);
        let select = format!("select * from zdb.highlight_fuzzy('idxtest_highlighting_{}', 'test_field', 'coal colt cot cheese beer co beer colter cat bolt', 'cet', -4) order by position;", title);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------

            let expect = vec![];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_two_term() -> spi::Result<()> {
        let title = "highlight_proximity_two_term";
        start_table_and_index(title);
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}],
                "distance": { "distance": 0, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','this is a test that is longer and has a second this near test a second time and a third this that is not' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            // test_field | this | <ALPHANUM> |       12 |           47 |         51
            // test_field | test | <ALPHANUM> |       14 |           57 |         61
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "this", 12, 47, 51),
                ("<ALPHANUM>", "test", 14, 57, 61),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_three_term() -> spi::Result<()> {
        let title = "highlight_proximity_three_term";
        start_table_and_index(title);
        let search_string = "this is a test that is longer and has a second this near test a second time and a third this that is not";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}],
                "distance": { "distance": 0, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["that", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // ------------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            // test_field | that | <ALPHANUM> |        5 |           15 |         19

            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "that", 5, 15, 19),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_one_term() -> spi::Result<()> {
        let title = "highlight_proximity_one_term";
        start_table_and_index(title);
        let search_string = "this is a test that is longer and has a second this near test a second time and a third this that is not";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart]) order by position;", title, search_string, array_one);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | this | <ALPHANUM> |       12 |           47 |         51
            // test_field | this | <ALPHANUM> |       21 |           88 |         92

            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "this", 12, 47, 51),
                ("<ALPHANUM>", "this", 21, 88, 92),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_three_term_found_twice() -> spi::Result<()> {
        let title = "highlight_proximity_three_term_twice";
        start_table_and_index(title);
        let search_string = "this is a test that is longer and has a second this near test a second time and a third that is not this test that whatever ";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}],
                "distance": { "distance": 0, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["that", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            // test_field | that | <ALPHANUM> |        5 |           15 |         19
            // test_field | this | <ALPHANUM> |       24 |          100 |        104
            // test_field | test | <ALPHANUM> |       25 |          105 |        109
            // test_field | that | <ALPHANUM> |       26 |          110 |        114

            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "that", 5, 15, 19),
                ("<ALPHANUM>", "this", 24, 100, 104),
                ("<ALPHANUM>", "test", 25, 105, 109),
                ("<ALPHANUM>", "that", 26, 110, 114),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_simple_in_order_test() -> spi::Result<()> {
        let title = "highlight_proximity_simple_in_order_test";
        start_table_and_index(title);
        let search_string = "this is this";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["is", null]}],
                "distance": { "distance": 0, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | is   | <ALPHANUM> |        2 |            5 |          7

            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_simple_without_order_test() -> spi::Result<()> {
        let title = "highlight_proximity_simple_without_order_test";
        start_table_and_index(title);
        let search_string = "this is this";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["is", null]}],
                "distance": { "distance": 0, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | is   | <ALPHANUM> |        2 |            5 |          7
            // test_field | this | <ALPHANUM> |        3 |            8 |         12

            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "this", 3, 8, 12),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_four_with_inorder_and_not_inorder() -> spi::Result<()> {
        let title = "highlight_proximity_four_with_inorder_and_not_inorder";
        start_table_and_index(title);
        let search_string = "now is the time for all good men to come to the aid of their country.";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["for", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["men", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["to", null]}],
                "distance": { "distance": 12, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_four = serde_json::to_string(&json! {
            {
                "words": [{"String":["now", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three, array_four);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | now  | <ALPHANUM> |        1 |            0 |          3
            // test_field | for  | <ALPHANUM> |        5 |           16 |         19
            // test_field | men  | <ALPHANUM> |        8 |           29 |         32
            // test_field | to   | <ALPHANUM> |        9 |           33 |         35
            // test_field | to   | <ALPHANUM> |       11 |           41 |         43
            let expect = vec![
                ("<ALPHANUM>", "now", 1, 0, 3),
                ("<ALPHANUM>", "for", 5, 16, 19),
                ("<ALPHANUM>", "men", 8, 29, 32),
                ("<ALPHANUM>", "to", 9, 33, 35),
                ("<ALPHANUM>", "to", 11, 41, 43),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlight_prox4_with_ord_and_non_ord_doubles_far_apart() -> spi::Result<()> {
        let title = "highlight_proximity_four_with_inorder_and_not_inorder_double";
        start_table_and_index(title);
        let search_string = "now is the time of the year for all good men to rise up and come to the aid of their country.";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["for", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["men", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["to", null]}],
                "distance": { "distance": 11, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_four = serde_json::to_string(&json! {
            {
                "words": [{"String":["now", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three, array_four);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | now  | <ALPHANUM> |        1 |            0 |          3
            // test_field | for  | <ALPHANUM> |        8 |           28 |         31
            // test_field | men  | <ALPHANUM> |       11 |           41 |         44
            // test_field | to   | <ALPHANUM> |       12 |           45 |         47
            // test_field | to   | <ALPHANUM> |       17 |           65 |         67
            let expect = vec![
                ("<ALPHANUM>", "now", 1, 0, 3),
                ("<ALPHANUM>", "for", 8, 28, 31),
                ("<ALPHANUM>", "men", 11, 41, 44),
                ("<ALPHANUM>", "to", 12, 45, 47),
                ("<ALPHANUM>", "to", 17, 65, 67),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_long_test() -> spi::Result<()> {
        let title = "highlight_proximity_long_test";
        start_table_and_index(title);
        let search_string = test_blurb();
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["energy", null]}],
                "distance": { "distance": 3, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["enron", null]}],
                "distance": { "distance": 3, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["lay", null]}],
                "distance": { "distance": 3, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+-----------

            let expect = vec![
                ("<ALPHANUM>", "energy", 224, 1597, 1603),
                ("<ALPHANUM>", "lay", 227, 1631, 1634),
                ("<ALPHANUM>", "enron", 228, 1648, 1653),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn itest_highlighter_proximity_array_two_then_one_in_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_two_then_one_in_order";
        start_table_and_index(title);
        let search_string = "This is a test";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["is", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | is   | <ALPHANUM> |        2 |            5 |          7
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_one_then_two_in_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_two_then_one_in_order";
        start_table_and_index(title);
        let search_string = "This is a test";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}, {"String":["is", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | is   | <ALPHANUM> |        2 |            5 |          7
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_two_then_one_without_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_two_then_one_without_order";
        start_table_and_index(title);
        let search_string = "This is a test";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["is", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | is   | <ALPHANUM> |        2 |            5 |          7
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_one_then_two_without_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_two_then_one_without_order";
        start_table_and_index(title);
        let search_string = "This is a test";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["is", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this | <ALPHANUM> |        1 |            0 |          4
            // test_field | is   | <ALPHANUM> |        2 |            5 |          7
            // test_field | test | <ALPHANUM> |        4 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_two_then_two_in_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_two_then_two_in_order";
        start_table_and_index(title);
        let search_string = "This is a test that is a bit longer";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["is", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}, {"String":["longer", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this   | <ALPHANUM> |        1 |            0 |          4
            // test_field | is     | <ALPHANUM> |        2 |            5 |          7
            // test_field | test   | <ALPHANUM> |        4 |           10 |         14
            // test_field | is     | <ALPHANUM> |        6 |           20 |         22
            // test_field | longer | <ALPHANUM> |        9 |           29 |         35
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "is", 6, 20, 22),
                ("<ALPHANUM>", "longer", 9, 29, 35),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_two_then_two_without_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_two_then_two_without_order";
        start_table_and_index(title);
        let search_string = "This is a test that is a bit longer";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["that", null]}, {"String":["longer", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}, {"String":["is", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            //  test_field | is     | <ALPHANUM> |        2 |            5 |          7
            //  test_field | test   | <ALPHANUM> |        4 |           10 |         14
            //  test_field | that   | <ALPHANUM> |        5 |           15 |         19
            //  test_field | is     | <ALPHANUM> |        6 |           20 |         22
            //  test_field | longer | <ALPHANUM> |        9 |           29 |         35
            let expect = vec![
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "that", 5, 15, 19),
                ("<ALPHANUM>", "is", 6, 20, 22),
                ("<ALPHANUM>", "longer", 9, 29, 35),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_three_then_three_in_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_three_then_three_in_order";
        start_table_and_index(title);
        let search_string =
            "This is a test that is a bit longer. I have also added another sentence to test.";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["longer", null]}, {"String": ["sentence", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}, {"String":["is", null]}, {"String": ["to", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this     | <ALPHANUM> |        1 |            0 |          4
            // test_field | is       | <ALPHANUM> |        2 |            5 |          7
            // test_field | test     | <ALPHANUM> |        4 |           10 |         14
            // test_field | sentence | <ALPHANUM> |       15 |           58 |         66
            // test_field | to       | <ALPHANUM> |       16 |           67 |         69
            // test_field | test     | <ALPHANUM> |       17 |           70 |         74
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "sentence", 15, 63, 71),
                ("<ALPHANUM>", "to", 16, 72, 74),
                ("<ALPHANUM>", "test", 17, 75, 79),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_three_then_three_without_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_three_then_three_without_order";
        start_table_and_index(title);
        let search_string =
            "This is a test that is a bit longer. I have also added another sentence to test.";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["longer", null]}, {"String": ["sentence", null]}],
                "distance": { "distance": 2, "in_order": false }
            }
        })
            .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["test", null]}, {"String":["is", null]}, {"String": ["to", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | this     | <ALPHANUM> |        1 |            0 |          4
            // test_field | is       | <ALPHANUM> |        2 |            5 |          7
            // test_field | test     | <ALPHANUM> |        4 |           10 |         14
            // test_field | is       | <ALPHANUM> |        6 |           20 |         22
            // test_field | longer   | <ALPHANUM> |        9 |           29 |         35
            // test_field | sentence | <ALPHANUM> |       15 |           63 |         71
            // test_field | to       | <ALPHANUM> |       16 |           72 |         74
            // test_field | test     | <ALPHANUM> |       17 |           75 |         79
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "test", 4, 10, 14),
                ("<ALPHANUM>", "is", 6, 20, 22),
                ("<ALPHANUM>", "longer", 9, 29, 35),
                ("<ALPHANUM>", "sentence", 15, 63, 71),
                ("<ALPHANUM>", "to", 16, 72, 74),
                ("<ALPHANUM>", "test", 17, 75, 79),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_three_then_three() -> spi::Result<()> {
        let title = "highlight_proximity_array_three_then_three_then_three";
        start_table_and_index(title);
        let search_string =
            "This is a test that is a bit longer. I have also added another sentence to test.";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["longer", null]}, {"String": ["sentence", null]}],
                "distance": { "distance": 2, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["is", null]}, {"String":["have", null]}, {"String": ["to", null]}],
                "distance": { "distance": 0, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["a", null]}, {"String":["test", null]}, {"String": ["also", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            //  field_name | term |    type    | position | start_offset | end_offset
            //  -----------+------+------------+----------+--------------+------------
            //  test_field | this     | <ALPHANUM> |        1 |            0 |          4
            //  test_field | is       | <ALPHANUM> |        2 |            5 |          7
            //  test_field | a        | <ALPHANUM> |        3 |            8 |          9
            //  test_field | longer   | <ALPHANUM> |        9 |           29 |         35
            //  test_field | have     | <ALPHANUM> |       11 |           39 |         43
            //  test_field | also     | <ALPHANUM> |       12 |           44 |         48
            //  test_field | sentence | <ALPHANUM> |       15 |           63 |         71
            //  test_field | to       | <ALPHANUM> |       16 |           72 |         74
            //  test_field | test     | <ALPHANUM> |       17 |           75 |         79
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "a", 3, 8, 9),
                ("<ALPHANUM>", "longer", 9, 29, 35),
                ("<ALPHANUM>", "have", 11, 39, 43),
                ("<ALPHANUM>", "also", 12, 44, 48),
                ("<ALPHANUM>", "sentence", 15, 63, 71),
                ("<ALPHANUM>", "to", 16, 72, 74),
                ("<ALPHANUM>", "test", 17, 75, 79),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_three_then_three_no_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_three_then_three_then_three_no_order";
        start_table_and_index(title);
        let search_string =
            "This is a test that is a bit longer. I have also added another sentence to test.";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["this", null]}, {"String":["longer", null]}, {"String": ["sentence", null]}],
                "distance": { "distance": 0, "in_order": false }
            }
        })
            .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["is", null]}, {"String":["have", null]}, {"String": ["another", null]}],
                "distance": { "distance": 0, "in_order": false }
            }
        })
            .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["a", null]}, {"String":["test", null]}, {"String": ["added", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
            .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            //  field_name | term |    type    | position | start_offset | end_offset
            //  -----------+------+------------+----------+--------------+------------
            //  test_field | this     | <ALPHANUM> |        1 |            0 |          4
            //  test_field | is       | <ALPHANUM> |        2 |            5 |          7
            //  test_field | a        | <ALPHANUM> |        3 |            8 |          9
            //  test_field | added    | <ALPHANUM> |       13 |           49 |         54
            //  test_field | another  | <ALPHANUM> |       14 |           55 |         62
            //  test_field | sentence | <ALPHANUM> |       15 |           63 |         71
            let expect = vec![
                ("<ALPHANUM>", "this", 1, 0, 4),
                ("<ALPHANUM>", "is", 2, 5, 7),
                ("<ALPHANUM>", "a", 3, 8, 9),
                ("<ALPHANUM>", "added", 13, 49, 54),
                ("<ALPHANUM>", "another", 14, 55, 62),
                ("<ALPHANUM>", "sentence", 15, 63, 71),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_highlighter_proximity_array_two_then_three_no_order() -> spi::Result<()> {
        let title = "highlight_proximity_array_three_then_three_then_three_no_order";
        start_table_and_index(title);
        let search_string =
            "Okay sure, I think this sentence is about fifteen words long Maybe less I dunno";
        let array_one = serde_json::to_string(&json! {
            {
                "words": [{"String":["okay", null]}, {"String":["sure", null]}],
                "distance": { "distance": 15, "in_order": false }
            }
        })
        .expect("failed to parse json");
        let array_two = serde_json::to_string(&json! {
            {
                "words": [{"String":["is", null]}, {"String":["fifteen", null]}, {"String": ["words", null]}],
                "distance": { "distance": 15, "in_order": false }
            }
        })
            .expect("failed to parse json");
        let array_three = serde_json::to_string(&json! {
            {
                "words": [{"String":["dunno", null]}],
                "distance": { "distance": 5, "in_order": true }
            }
        })
        .expect("failed to parse json");
        let select = format!("select * from zdb.highlight_proximity('idxtest_highlighting_{}', 'test_field','{}' ,  ARRAY['{}'::proximitypart, '{}'::proximitypart, '{}'::proximitypart]) order by position;", title, search_string, array_one, array_two, array_three);
        Spi::connect(|client| {
            let table = client.select(&select, None, None)?;

            // field_name | term |    type    | position | start_offset | end_offset
            // -----------+------+------------+----------+--------------+------------
            // test_field | okay    | <ALPHANUM> |        1 |            0 |          4
            // test_field | sure    | <ALPHANUM> |        2 |            5 |          9
            // test_field | is      | <ALPHANUM> |        7 |           33 |         35
            // test_field | fifteen | <ALPHANUM> |        9 |           42 |         49
            // test_field | words   | <ALPHANUM> |       10 |           50 |         55
            // test_field | dunno   | <ALPHANUM> |       15 |           74 |         79
            let expect = vec![
                ("<ALPHANUM>", "okay", 1, 0, 4),
                ("<ALPHANUM>", "sure", 2, 5, 9),
                ("<ALPHANUM>", "is", 7, 33, 35),
                ("<ALPHANUM>", "fifteen", 9, 42, 49),
                ("<ALPHANUM>", "words", 10, 50, 55),
                ("<ALPHANUM>", "dunno", 15, 74, 79),
            ];

            test_table(table, expect);

            Ok(())
        })
    }

    fn start_table_and_index(title: &str) {
        let create_table = &format!(
            "CREATE TABLE test_highlighting_{} AS SELECT * FROM generate_series(1, 10);",
            title,
        );
        Spi::run(create_table).expect("SPI failed");
        let create_index = &format!("CREATE INDEX idxtest_highlighting_{} ON test_highlighting_{} USING zombodb ((test_highlighting_{}.*))", title, title, title, );
        Spi::run(create_index).expect("SPI failed");
    }

    fn test_table(mut table: SpiTupleTable, expect: Vec<(&str, &str, i32, i64, i64)>) {
        let mut i = 0;
        while let Some(_) = table.next() {
            let token = table.get::<&str>(2).expect("SPI failed").unwrap();
            let ttype = table.get::<&str>(3).expect("SPI failed").unwrap();
            let pos = table.get::<i32>(4).expect("SPI failed").unwrap();
            let start_offset = table.get::<i64>(5).expect("SPI failed").unwrap();
            let end_offset = table.get::<i64>(6).expect("SPI failed").unwrap();

            let row_tuple = (ttype, token, pos, start_offset, end_offset);

            assert_eq!(expect[i], row_tuple);

            i += 1;
        }
        assert_eq!(expect.len(), i);
    }

    fn test_blurb() -> String {
        let blurb = "Enron

        P.O.Box 1188
        Houston, TX 77251-1188

        Mark Palmer
        713-853-4738

        ENRON REPORTS RECURRING THIRD QUARTER EARNINGS OF $0.43 PER
        DILUTED SHARE; REPORTS NON-RECURRING CHARGES OF $1.01 BILLION
        AFTER-TAX; REAFFIRMS RECURRING EARNINGS ESTIMATES OF $1.80 FOR
        2001 AND $2.15 FOR 2002; AND EXPANDS FINANCIAL REPORTING

        FOR IMMEDIATE RELEASE:  Tuesday, Oct. 16, 2001

        HOUSTON - Enron Corp. (NYSE - ENE) announced today recurring earnings per
        diluted share of $0.43 for the third quarter of 2001, compared to $0.34 a year ago.  Total
        recurring net income increased to $393 million, versus $292 million a year ago.
            Our 26 percent increase in recurring earnings per diluted share shows the very strong
        results of our core wholesale and retail energy businesses and our natural gas pipelines,   said
        Kenneth L. Lay, Enron chairman and CEO.   The continued excellent prospects in these
        businesses and Enron ''s leading market position make us very confident in our strong earnings
        outlook.
            Non-recurring charges totaling $1.01 billion after-tax, or $(1.11) loss per diluted share,
        were recognized for the third quarter of 2001.  The total net loss for the quarter, including non-
            recurring items, was $(618) million, or $(0.84) per diluted share.
            After a thorough review of our businesses, we have decided to take these charges to
        clear away issues that have clouded the performance and earnings potential of our core energy
        businesses,   said Lay.
            Enron also reaffirmed today it is on track to continue strong earnings growth and achieve
        its previously stated targets of recurring earnings per diluted share of  $0.45 for the fourth
        quarter 2001, $1.80 for 2001 and $2.15 for 2002.
        PERFORMANCE SUMMARY
        Enron has recently expanded the reporting of its financial results by both providing
        additional segments and expanding financial and operating information in the attached tables.
            Enron ''s business segments are as follows:
        ?  Wholesale Services
        o Americas
        o Europe and Other Commodity Markets
            ?  Retail Services
            ?  Transportation and Distribution
        o Natural Gas Pipelines
        o Portland General
        o Global Assets
            ?  Broadband Services
            ?  Corporate and Other

        Wholesale Services:  Total income before interest, minority interests and taxes (IBIT)
        increased 28 percent to $754 million in the third quarter of 2001, compared to $589 million in
        the third quarter of last year.  Total wholesale physical volumes increased 65 percent to 88.2
        trillion British thermal units equivalent per day (Tbtue/d) in the recent quarter.
            Americas  - This segment consists of Enron ''s gas and power market-making operations
        and merchant energy activities in North and South America.  IBIT from this segment grew 31
        percent to $701 million in the recent quarter from $536 million a year ago, driven by strong
        results from the North America natural gas and power businesses.  Natural gas volumes
        increased 6 percent to 26.7 Tbtu/d, and power volumes increased 77 percent to 290 million
        megawatt-hours (MWh).
            Europe and Other Commodity Markets - This segment includes Enron ''s European gas
        and power operations and Enron ''s other commodity businesses, such as metals, coal, crude and
        liquids, weather, forest products and steel.  For the third quarter of 2001, IBIT for the segment
        remained unchanged at $53 million as compared to last year.  Although physical volumes
        increased for each commodity in the segment, the low level of volatility in the gas and power
        markets caused profitability to remain flat.

            Retail Services:  Enron ''s Retail Services product offerings include pricing and delivery
        of natural gas and power, as well as demand side management services to minimize energy costs
        for business consumers in North America and Europe.  In the third quarter of 2001, Retail
        Services generated IBIT of $71 million, compared to $27 million a year ago.  Retail Services
        continues to successfully penetrate markets with standard, scalable products to reduce
        consumers '' total energy costs.  Enron recently added new business with large consumers,
        including Wal-Mart, Northrop Grumman, the City of Chicago, Equity Office Properties and
        Wendy ''s in the U.S. and Sainsbury and Guinness Brewery in the U.K.  To date in 2001, Enron
        has completed over 50 transactions with large consumers.  Enron is also successfully extending
        its retail energy products to small business customers, completing over 95,000 transactions in the
        first nine months of this year.
            Transportation and Distribution:  The Transportation and Distribution group includes
        Natural Gas Pipelines, Portland General and Global Assets.
            Natural Gas Pipelines - This segment provided $85 million of IBIT in the current
        quarter, up slightly from the same quarter last year.  Pipeline expansions are underway in high
        growth areas and include a 428 million cubic feet per day (MMcf/d) expansion by Florida Gas
        Transmission and a 150 MMcf/d expansion by Transwestern.
            Portland General - Portland General Electric, an electric utility in the northwestern U.S.,
        reported an IBIT loss of $(17) million compared to IBIT of $74 million in the same quarter a
        year ago.  Portland General entered into power contracts in prior periods to ensure adequate
        supply for the recent quarter at prices that were significantly higher than actual settled prices
        during the third quarter of 2001.  Although the rate mechanism in place anticipated and
        substantially mitigated the effect of the higher purchased power costs, only the amount in excess
        of a defined baseline was recoverable from ratepayers.  Increased power cost recovery was
        incorporated into Portland General ''s new fifteen-month rate structure, which became effective
        October 1, 2001 and included an average 40 percent rate increase.
            Last week, Enron announced a definitive agreement to sell Portland General to Northwest
        Natural Gas for approximately $1.9 billion and the assumption of approximately $1.1 billion in
        Portland General debt.  The proposed transaction, which is subject to customary regulatory
        approvals, is expected to close by late 2002.

        Global Assets - The Global Assets segment includes assets not part of Enron ''s wholesale
        or retail energy operations.  Major assets included in this segment are Elektro, an electric utility
        in Brazil; Dabhol, a power plant in India; TGS, a natural gas pipeline in Argentina; Azurix; and
        the Enron Wind operations.  For the third quarter of 2001, IBIT for the segment remained
        unchanged at $19 million as compared to last year.
            Broadband Services:  Enron makes markets for bandwidth, IP and storage products and
        bundles such products for comprehensive network management services.  IBIT losses were $(80)
        million in the current quarter compared to a $(20) million loss in the third quarter of last year.
            This quarter ''s results include significantly lower investment-related income and lower operating
        costs.
            Corporate and Other:  Corporate and Other reported an IBIT loss of $(59) million for
        the quarter compared to $(106) million loss a year ago.  Corporate and Other represents the
        unallocated portion of expenses related to general corporate functions.

            NON-RECURRING ITEMS
        Enron ''s results in the third quarter of 2001 include after-tax non-recurring charges of
        $1.01 billion, or $(1.11) per diluted share, consisting of:
        ?  $287 million related to asset impairments recorded by Azurix Corp.  These
        impairments primarily reflect Azurix ''s planned disposition of its North American
        and certain South American service-related businesses;
        ?  $180 million associated with the restructuring of Broadband Services, including
        severance costs, loss on the sale of inventory and an impairment to reflect the
        reduced value of Enron ''s content services business; and
            ?  $544 million related to losses associated with certain investments, principally
        Enron ''s interest in The New Power Company, broadband and technology
        investments, and early termination during the third quarter of certain structured
        finance arrangements with a previously disclosed entity.


            OTHER INFORMATION
        A conference call with Enron management regarding third quarter results will be
        conducted live today at 10:00 a.m. EDT and may be accessed through the Investor Relations
        page at www.enron.com
            .
                Enron is one of the world ''s leading energy, commodities and service companies.  The
        company makes markets in electricity and natural gas, delivers energy and other physical
        commodities, and provides financial and risk management services to customers around the
        world.  The stock is traded under the ticker symbol   ENE.
            ______________________________________________________________________________
        Please see attached tables for additional financial information.

            This press release includes forward-looking statements within the meaning of Section 27A of the Securities Act of 1933 and Section
        21E of the Securities Exchange Act of 1934.  The Private Securities Litigation Reform Act of 1995 provides a safe harbor for forward-looking
        statements made by Enron or on its behalf.  These forward-looking statements are not historical facts, but reflect Enron ''s curr ent expectations,
        estimates and projections.  All statements contained in the press release which address future operating performance, events or developments that
        are expected to occur in the future (including statements relating to earnings expectations, sales of assets, or statements expressing general
        optimism about future operating results) are forward-looking statements.  Although Enron believes that its expectations are bas ed on reasonable
        assumptions, it can give no assurance that its goals will be achieved.  Important factors that could cause actual results to di ffer materially from
        those in the forward-looking statements herein include success in marketing natural gas and power to wholesale customers; the ability to
        penetrate new retail natural gas and electricity markets, including the energy outsource market, in the United States and Europe; the timing, extent
        and market effects of deregulation of energy markets in the United States and in foreign jurisdictions; development of Enron ''s broadband
        network and customer demand for intermediation and content services; political developments in foreign countries; receipt of re gulatory
        approvals and satisfaction of customary closing conditions to the sale of Portland General; and conditions of the capital markets and equity
        markets during the periods covered by the forward-looking statements.";
        String::from(blurb)
    }
}
