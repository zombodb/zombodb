use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use serde::de::DeserializeOwned;
use serde::export::PhantomData;
use serde::*;
use serde_json::*;

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
        query: ZDBQuery,
        agg_json: serde_json::Value,
    ) -> ElasticsearchAggregateSearchRequest<ReturnType> {
        let query_dsl = apply_visibility_clause(&elasticsearch, &query, false);
        ElasticsearchAggregateSearchRequest::<ReturnType> {
            elasticsearch: elasticsearch.clone(),
            json_query: json! {
                {
                    "query": query_dsl,
                    "aggs": {
                        "the_agg": agg_json
                    }
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
            struct TheAgg {
                the_agg: serde_json::Value,
            }

            #[derive(Deserialize)]
            struct AggregateResponse {
                #[serde(rename = "_shards")]
                shards: Shards,
                aggregations: TheAgg,
            }

            let agg_resp: AggregateResponse =
                serde_json::from_str(&body).expect("received invalid aggregate json response");
            let the_agg = agg_resp.aggregations.the_agg;
            Ok(serde_json::from_value::<ReturnType>(the_agg).unwrap())
        })
    }
}
