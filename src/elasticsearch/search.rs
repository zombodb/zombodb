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

pub struct Scroller {
    receiver: crossbeam::channel::Receiver<(f64, u64)>,
}

impl Scroller {
    fn new(
        elasticsearch: Elasticsearch,
        mut scroll_id: Option<String>,
        iter: std::vec::IntoIter<InnerHit>,
    ) -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let (scroll_sender, scroll_receiver) = crossbeam::channel::unbounded();

        // go ahead and queue up the results we currently have from the initial search
        scroll_sender
            .send(iter)
            .expect("scroll_sender channel is closed");

        // spawn a thread to continually get the next scroll chunk from Elasticsearch
        // until there's no more to get
        std::thread::spawn(move || {
            std::panic::catch_unwind(|| {
                while let Some(sid) = scroll_id {
                    match ElasticsearchSearchRequest::scroll(elasticsearch.clone(), &sid) {
                        Ok(response) => {
                            scroll_id = response.scroll_id;

                            match response.hits.hits {
                                Some(inner_hits) => {
                                    // send the hits across the scroll_sender channel
                                    // so they can be iterated from another thread
                                    scroll_sender
                                        .send(inner_hits.into_iter())
                                        .expect("failed to send iter over scroll_sender");
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }

                    if scroll_id.is_none() {
                        break;
                    }
                }
            })
            .ok();
        });

        // this thread sends the hits back to the main thread across the 'sender/receiver' channel
        // until there's no more to send
        std::thread::spawn(move || {
            std::panic::catch_unwind(|| {
                for itr in scroll_receiver {
                    for hit in itr {
                        sender
                            .send((hit.score, hit.fields.zdb_ctid[0]))
                            .expect("failed to send hit over sender");
                    }
                }
            })
            .ok();
        });

        Scroller { receiver }
    }

    fn next(&self) -> Option<(f64, u64)> {
        match self.receiver.recv() {
            Ok(tuple) => Some(tuple),
            Err(_) => None,
        }
    }
}

pub struct SearchResponseIntoIter {
    scroller: Scroller,
}

impl Iterator for SearchResponseIntoIter {
    type Item = (f64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.scroller.next()
    }
}

impl IntoIterator for ElasticsearchSearchResponse {
    type Item = (f64, u64);
    type IntoIter = SearchResponseIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        SearchResponseIntoIter {
            scroller: Scroller::new(
                self.elasticsearch.expect("no elasticsearch"),
                self.scroll_id,
                self.hits.hits.unwrap_or_default().into_iter(),
            ),
        }
    }
}
