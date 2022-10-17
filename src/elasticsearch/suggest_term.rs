use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::{ZDBPreparedQuery, ZDBQuery};
use pgx::{prelude::*, *};
use serde::*;
use serde_json::*;

#[derive(Deserialize)]
pub struct SuggestTermsOptions {
    text: String,
    score: f64,
    freq: usize,
}

#[derive(Deserialize)]
pub struct SuggestTermsResponse {
    text: String,
    offset: usize,
    length: usize,
    options: Vec<SuggestTermsOptions>,
}

pub struct ElasticsearchSuggestTermRequest {
    elasticsearch: Elasticsearch,
    query: ZDBPreparedQuery,
    suggest: String,
    fieldname: String,
}

impl ElasticsearchSuggestTermRequest {
    pub fn new(
        elasticsearch: &Elasticsearch,
        query: ZDBPreparedQuery,
        fieldname: String,
        suggest: String,
    ) -> Self {
        ElasticsearchSuggestTermRequest {
            elasticsearch: elasticsearch.clone(),
            query,
            fieldname,
            suggest,
        }
    }

    pub fn execute(self) -> std::result::Result<Vec<SuggestTermsResponse>, ElasticsearchError> {
        let body = {
            json! {
                {
                    "query": apply_visibility_clause(&self.elasticsearch, self.query, false),
                    "suggest" : {
                        "suggestion" : {
                            "text" : self.suggest,
                            "term" : {
                                "field" : self.fieldname
                            }
                        }
                    }
                }
            }
        };

        let mut url = self.elasticsearch.alias_url();
        url.push_str("/_search?size=0");
        Elasticsearch::execute_json_request(
            Elasticsearch::client().post(&url),
            Some(body),
            |body| {
                #[derive(Deserialize)]
                #[serde(rename(deserialize = "suggest"))]
                struct Suggest {
                    suggestion: Vec<SuggestTermsResponse>,
                }

                #[derive(Deserialize)]
                struct WholeResponse {
                    suggest: Suggest,
                }

                let response: WholeResponse = serde_json::from_reader(body)
                    .expect("failed to deserialize suggest terms response");
                Ok(response.suggest.suggestion)
            },
        )
    }
}

#[pg_extern(immutable, parallel_safe)]
fn suggest_terms(
    index: PgRelation,
    field_name: String,
    suggest: String,
    query: ZDBQuery,
) -> TableIterator<
    'static,
    (
        name!(term, String),
        name!(offset, i64),
        name!(length, i64),
        name!(suggestion, String),
        name!(score, f64),
        name!(frequency, i64),
    ),
> {
    let (prepared_query, index) = query.prepare(&index, Some(field_name.clone()));

    let elasticsearch = Elasticsearch::new(&index);
    let results = elasticsearch
        .suggest_terms(prepared_query, field_name, suggest)
        .execute()
        .expect("failed to suggest terms");

    TableIterator::new(
        results
            .iter()
            .map(|terms| {
                terms.options.iter().map(move |opts| {
                    (
                        terms.text.clone(),
                        terms.offset as i64,
                        terms.length as i64,
                        opts.text.clone(),
                        opts.score,
                        opts.freq as i64,
                    )
                })
            })
            .flatten()
            .collect::<Vec<_>>()
            .into_iter(),
    )
}
