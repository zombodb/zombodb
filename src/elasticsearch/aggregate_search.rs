use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBPreparedQuery;
use serde::de::DeserializeOwned;
use serde::export::PhantomData;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

pub struct ElasticsearchAggregateSearchRequest<ReturnType>
where
    ReturnType: DeserializeOwned,
{
    elasticsearch: Elasticsearch,
    json_query: serde_json::Value,
    _marker: PhantomData<ReturnType>,
}

impl<ReturnType> ElasticsearchAggregateSearchRequest<ReturnType>
where
    ReturnType: DeserializeOwned,
{
    pub fn new(
        elasticsearch: &Elasticsearch,
        query: ZDBPreparedQuery,
        agg_json: HashMap<String, serde_json::Value>,
    ) -> ElasticsearchAggregateSearchRequest<ReturnType> {
        let query_dsl = apply_visibility_clause(&elasticsearch, query, false);
        ElasticsearchAggregateSearchRequest::<ReturnType> {
            elasticsearch: elasticsearch.clone(),
            json_query: json! {
                {
                    "query": query_dsl,
                    "aggs": agg_json
                }
            },
            _marker: PhantomData::<ReturnType>,
        }
    }

    pub fn from_raw(
        elasticsearch: &Elasticsearch,
        agg_json: serde_json::Value,
    ) -> ElasticsearchAggregateSearchRequest<ReturnType> {
        ElasticsearchAggregateSearchRequest::<ReturnType> {
            elasticsearch: elasticsearch.clone(),
            json_query: json! {
                {
                    "aggs": {
                        "the_agg": agg_json
                    }
                }
            },
            _marker: PhantomData::<ReturnType>,
        }
    }

    pub fn execute(self) -> std::result::Result<ReturnType, ElasticsearchError> {
        let result = self.execute_set()?;
        Ok(result.0)
    }

    pub fn execute_set(
        self,
    ) -> std::result::Result<(ReturnType, HashMap<String, serde_json::Value>), ElasticsearchError>
    {
        let mut url = self.elasticsearch.alias_url();
        url.push_str("/_search");
        url.push_str("?size=0");

        let client = Elasticsearch::client()
            .get(&url)
            .header("content-type", "application/json")
            .body(serde_json::to_string(&self.json_query).unwrap());

        Elasticsearch::execute_request(client, |_, body| {
            #[derive(Deserialize)]
            struct Shards {
                total: u32,
                failed: u32,
                skipped: u32,
                successful: u32,
            }

            #[derive(Deserialize)]
            struct AggregateResponse {
                #[serde(rename = "_shards")]
                shards: Shards,
                aggregations: HashMap<String, serde_json::Value>,
            }

            let agg_resp: AggregateResponse =
                serde_json::from_str(&body).expect("received invalid aggregate json response");
            let mut aggregations = agg_resp.aggregations;
            let mut unrolled = HashMap::new();
            let mut unrolled_cnt = 0;

            // look for and pull out any nested aggregates to the top-level
            // and move everything into the "unrolled" Map
            loop {
                for (k, mut v) in aggregations {
                    if let Some(nested_agg) = v
                        .as_object_mut()
                        .unwrap_or_else(|| panic!("'{}' is not an object", k))
                        .remove(&k)
                    {
                        // we have a nested agg so that becomes the top-level
                        unrolled.insert(k, nested_agg);
                        unrolled_cnt += 1;
                    } else {
                        // nothing nested under this value, so use as-is
                        unrolled.insert(k, v);
                    }
                }

                if unrolled_cnt > 0 {
                    // keep unrolling
                    unrolled_cnt = 0;
                    aggregations = unrolled;
                    unrolled = HashMap::new();
                } else {
                    // we're done
                    break;
                }
            }

            let the_agg = unrolled.remove("the_agg").expect("didn't find 'the_agg'");
            let data = serde_json::from_value::<ReturnType>(the_agg)
                .expect("failed to deserialize 'the_agg' response");
            Ok((data, unrolled))
        })
    }
}
