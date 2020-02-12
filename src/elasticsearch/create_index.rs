use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::mapping::lookup_analysis_thing;
use serde_json::*;

pub struct ElasticsearchCreateIndexRequest<'a> {
    elasticsearch: &'a Elasticsearch<'a>,
    mapping: Value,
}

impl<'a> ElasticsearchCreateIndexRequest<'a> {
    pub fn new(elasticsearch: &'a Elasticsearch, mapping: Value) -> Self {
        ElasticsearchCreateIndexRequest {
            elasticsearch,
            mapping,
        }
    }

    pub fn execute(self) -> std::result::Result<(), ElasticsearchError> {
        Elasticsearch::execute_request(
            reqwest::Client::new()
                .put(&self.elasticsearch.base_url())
                .header("content-type", "application/json")
                .body(serde_json::to_string(&self.create_request_body()).unwrap()),
            |_, _| Ok(()),
        )
    }

    fn create_request_body(&self) -> Value {
        json! {
            {
               "settings": {
                  "number_of_shards": self.elasticsearch.options.shards(),
                  "index.number_of_replicas": 0,
                  "index.refresh_interval": "-1",
                  "index.query.default_field": "zdb_all",
                  "analysis": {
                     "filter": lookup_analysis_thing("filters"),
                     "char_filter" : lookup_analysis_thing("char_filters"),
                     "tokenizer" : lookup_analysis_thing("tokenizers"),
                     "analyzer": lookup_analysis_thing("analyzers"),
                     "normalizer": lookup_analysis_thing("normalizers")
                  }
               },
               "mappings": {
                     "_source": { "enabled": true },
                     "dynamic_templates": [
                          {
                             "strings": {
                                "match_mapping_type": "string",
                                "mapping": {
                                   "type": "keyword",
                                   "ignore_above": 10922,
                                   "normalizer": "lowercase",
                                   "copy_to": "zdb_all"
                                 }
                              }
                          },
                          {
                             "dates_times": {
                                "match_mapping_type": "date",
                                "mapping": {
                                   "type": "date",
                                   "format": "strict_date_optional_time||epoch_millis||HH:mm:ss.SSSSSS||HH:mm:ss.SSSSSSZZ",
                                   "copy_to": "zdb_all"
                                 }
                              }
                          }
                     ],
                     "properties": self.mapping
               },
               "aliases": {
                  self.elasticsearch.options.alias(self.elasticsearch.heaprel, self.elasticsearch.indexrel): {}
               }
            }
        }
    }
}
