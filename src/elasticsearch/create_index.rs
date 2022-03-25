use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::mapping::lookup_analysis_thing;
use serde_json::*;

pub struct ElasticsearchCreateIndexRequest {
    elasticsearch: Elasticsearch,
    mapping: Value,
}

impl ElasticsearchCreateIndexRequest {
    pub fn new(elasticsearch: &Elasticsearch, mapping: Value) -> Self {
        ElasticsearchCreateIndexRequest {
            elasticsearch: elasticsearch.clone(),
            mapping,
        }
    }

    pub fn execute(self) -> std::result::Result<(), ElasticsearchError> {
        Elasticsearch::execute_json_request(
            Elasticsearch::client().put(&self.elasticsearch.base_url()),
            Some(self.create_request_body()),
            |_| Ok(()),
        )?;

        let url = format!(
            "{}_cluster/health/{}?wait_for_active_shards={}&timeout=5m&master_timeout=5m",
            self.elasticsearch.url(),
            self.elasticsearch.index_name(),
            self.elasticsearch.options.shards()
        );

        Elasticsearch::execute_json_request(Elasticsearch::client().get(&url), None, |_| Ok(()))
            .expect("failed to wait for yellow status");

        Ok(())
    }

    fn create_request_body(&self) -> Value {
        // hacky way to see if the mapping contains a nested field
        let has_nested_field = serde_json::to_string(&self.mapping)
            .unwrap()
            .contains(r#""type":"nested""#);

        let index_block = if has_nested_field {
            // we can't do an index-level sort
            json! { {
              "number_of_shards": self.elasticsearch.options.shards(),
              "number_of_replicas": 0,
              "refresh_interval": "-1",
              "query.default_field": "zdb_all",
              "translog.durability": "async",
              "mapping.nested_fields.limit": self.elasticsearch.options.nested_fields_limit(),
              "mapping.nested_objects.limit": self.elasticsearch.options.nested_objects_limit(),
              "mapping.total_fields.limit": self.elasticsearch.options.total_fields_limit(),
              "max_result_window": self.elasticsearch.options.max_result_window(),
              "max_terms_count": self.elasticsearch.options.max_terms_count(),
              "analyze.max_token_count": self.elasticsearch.options.max_analyze_token_count()
            } }
        } else {
            // we can do an index-level sort on zdb_ctid:asc
            json! { {
              "number_of_shards": self.elasticsearch.options.shards(),
              "number_of_replicas": 0,
              "refresh_interval": "-1",
              "query.default_field": "zdb_all",
              "translog.durability": "async",
              "mapping.nested_fields.limit": self.elasticsearch.options.nested_fields_limit(),
              "mapping.nested_objects.limit": self.elasticsearch.options.nested_objects_limit(),
              "mapping.total_fields.limit": self.elasticsearch.options.total_fields_limit(),
              "max_result_window": self.elasticsearch.options.max_result_window(),
              "max_terms_count": self.elasticsearch.options.max_terms_count(),
              "analyze.max_token_count": self.elasticsearch.options.max_analyze_token_count(),
              "sort.field": "zdb_ctid",
              "sort.order": "asc"
            } }
        };

        let source = if self.elasticsearch.options.include_source() {
            json! { { "enabled": true } }
        } else {
            json! { { "includes": [ "zdb_aborted_xids"] } }
        };

        json! {
            {
               "settings": {
                  "index": index_block,
                  "analysis": {
                     "filter": lookup_analysis_thing("filters"),
                     "char_filter" : lookup_analysis_thing("char_filters"),
                     "tokenizer" : lookup_analysis_thing("tokenizers"),
                     "analyzer": lookup_analysis_thing("analyzers"),
                     "normalizer": lookup_analysis_thing("normalizers")
                  },
                 "similarity": lookup_analysis_thing("similarities")
               },
               "mappings": {
                     "_source": source,
                     "date_detection": self.elasticsearch.options.nested_object_date_detection(),
                     "numeric_detection": self.elasticsearch.options.nested_object_numeric_detection(),
                     "dynamic_templates": [
                          {
                             "strings": {
                                "match_mapping_type": "string",
                                "mapping": self.elasticsearch.options.nested_object_text_mapping()
                              }
                          },
                          {
                             "dates_times": {
                                "match_mapping_type": "date",
                                "mapping": {
                                   "type": "keyword",
                                   "copy_to": "zdb_all",
                                   "fields": {
                                     "date": {
                                        "type": "date",
                                        "format": "strict_date_optional_time||epoch_millis||HH:mm:ss.S||HH:mm:ss.SX||HH:mm:ss.SS||HH:mm:ss.SSX||HH:mm:ss.SSS||HH:mm:ss.SSSX||HH:mm:ss.SSSS||HH:mm:ss.SSSSX||HH:mm:ss.SSSSS||HH:mm:ss.SSSSSX||HH:mm:ss.SSSSSS||HH:mm:ss.SSSSSSX"                                      }
                                    }
                                 }
                              }
                          },
                          {
                            "objects": {
                                "match_mapping_type": "object",
                                "mapping": {
                                    "type": "nested",
                                    "include_in_parent": true
                                }
                            }
                          }
                     ],
                     "properties": self.mapping
               },
               "aliases": {
                  self.elasticsearch.options.alias(): {}
               }
            }
        }
    }
}
