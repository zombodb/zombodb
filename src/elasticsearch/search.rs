use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::ZDBQuery;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

const SEARCH_FILTER_PATH:&'static str = "_scroll_id,_shards.*,hits.total,hits.max_score,hits.hits._score,hits.hits.fields.*,hits.hits.highlight.*";

pub struct ElasticsearchSearchRequest {
    elasticsearch: Elasticsearch,
    query: ZDBQuery,
}

#[derive(Deserialize)]
pub struct Shards {
    total: Option<u64>,
    successful: Option<u64>,
    skipped: Option<u64>,
    failed: Option<u64>,
}

#[derive(Deserialize)]
pub struct HitsTotal {
    value: u64,
    relation: String,
}

#[derive(Deserialize)]
pub struct Fields {
    zdb_ctid: [u64; 1],

    #[serde(flatten)]
    other: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
pub struct InnerHit {
    #[serde(rename = "_index")]
    index: Option<String>,

    #[serde(rename = "_type")]
    type_: Option<String>,

    #[serde(rename = "_score")]
    score: f64,

    #[serde(rename = "_id")]
    id: Option<String>,

    fields: Fields,
}

#[derive(Deserialize)]
pub struct Hits {
    total: HitsTotal,
    max_score: Option<f64>,
    hits: Option<Vec<InnerHit>>,
}

#[derive(Deserialize)]
pub struct ElasticsearchSearchResponse {
    #[serde(skip)]
    elasticsearch: Option<Elasticsearch>,

    #[serde(rename = "_scroll_id")]
    scroll_id: Option<String>,

    #[serde(rename = "_shards")]
    shards: Shards,

    hits: Hits,
}

impl ElasticsearchSearchRequest {
    pub fn new(elasticsearch: &Elasticsearch, query: ZDBQuery) -> Self {
        ElasticsearchSearchRequest {
            elasticsearch: elasticsearch.clone(),
            query,
        }
    }

    pub fn execute(self) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        ElasticsearchSearchRequest::initial_search(self.elasticsearch, self.query)
    }

    fn initial_search(
        elasticsearch: Elasticsearch,
        query: ZDBQuery,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        let mut url = String::new();
        url.push_str(&elasticsearch.base_url());
        url.push_str("/_search");
        url.push_str("?search_type=query_then_fetch");
        url.push_str("&_source=false");
        url.push_str("&size=10000");
        url.push_str("&scroll=10m");
        url.push_str("&filter_path=");
        url.push_str(SEARCH_FILTER_PATH);
        url.push_str("&stored_fields=_none_");
        url.push_str("&docvalue_fields=zdb_ctid");

        ElasticsearchSearchRequest::get_hits(
            url,
            json! {
                {
                    "query": query.query_dsl().expect("zdbquery has None QueryDSL")
                }
            },
            elasticsearch,
        )
    }

    fn scroll(
        elasticsearch: Elasticsearch,
        scroll_id: &str,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        let mut url = String::new();
        url.push_str(&elasticsearch.options.url);
        url.push_str("_search/scroll");
        url.push_str("?filter_path=");
        url.push_str(SEARCH_FILTER_PATH);

        ElasticsearchSearchRequest::get_hits(
            url,
            json! {
                {
                    "scroll": "10m",
                    "scroll_id": scroll_id
                }
            },
            elasticsearch,
        )
    }

    fn get_hits(
        url: String,
        body: serde_json::Value,
        elasticsearch: Elasticsearch,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        Elasticsearch::execute_request(
            reqwest::Client::new()
                .post(&url)
                .header("content-type", "application/json")
                .body(serde_json::to_string(&body).unwrap()),
            |code, body| {
                let mut response = match serde_json::from_str::<ElasticsearchSearchResponse>(&body)
                {
                    Ok(json) => json,
                    Err(_) => {
                        return Err(ElasticsearchError(Some(code), body));
                    }
                };

                // assign a clone of our ES client to the response too,
                // for future use during iteration
                response.elasticsearch = Some(elasticsearch);

                Ok(response)
            },
        )
    }
}

pub struct SearchResponseIntoIter {
    elasticsearch: Elasticsearch,
    scroll_id: Option<String>,
    total_hits: u64,
    inner_hits_iter: std::vec::IntoIter<InnerHit>,
    curr: u64,
}

impl Iterator for SearchResponseIntoIter {
    type Item = (f64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.inner_hits_iter.next() {
            Some(inner_hit) => Some((inner_hit.score, inner_hit.fields.zdb_ctid[0])),
            None => {
                // go get next scroll chunk
                let response = ElasticsearchSearchRequest::scroll(
                    self.elasticsearch.clone(),
                    self.scroll_id.as_ref().expect("no scroll id"),
                )
                .expect("failed to get next set of hits");

                self.scroll_id = response.scroll_id;
                self.inner_hits_iter = match response.hits.hits {
                    Some(inner_hits) => inner_hits.into_iter(),
                    None => return None,
                };

                match self.inner_hits_iter.next() {
                    Some(inner_hit) => Some((inner_hit.score, inner_hit.fields.zdb_ctid[0])),
                    None => None,
                }
            }
        };

        self.curr += 1;
        item
    }
}

impl IntoIterator for ElasticsearchSearchResponse {
    type Item = (f64, u64);
    type IntoIter = SearchResponseIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let elasticsearch = self.elasticsearch.as_ref().unwrap().clone();
        SearchResponseIntoIter {
            elasticsearch,
            scroll_id: self.scroll_id,
            total_hits: self.hits.total.value,
            inner_hits_iter: self
                .hits
                .hits
                .expect("no inner hits in response")
                .into_iter(),
            curr: 0,
        }
    }
}
