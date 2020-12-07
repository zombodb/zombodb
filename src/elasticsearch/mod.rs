#![allow(dead_code)]

mod aggregates;
mod aliases;
pub(crate) mod analyze;
mod bulk;
mod cat;
mod count;
mod create_index;
mod delete_index;
mod expunge_deletes;
mod get_document;
mod get_mapping;
mod get_settings;
mod profile_query;
mod put_mapping;
mod refresh_index;
mod suggest_term;
mod update_settings;

pub mod aggregate_search;
pub mod search;

use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::aggregate_search::ElasticsearchAggregateSearchRequest;
use crate::elasticsearch::aliases::ElasticsearchAliasRequest;
use crate::elasticsearch::analyze::ElasticsearchAnalyzerRequest;
use crate::elasticsearch::cat::ElasticsearchCatRequest;
use crate::elasticsearch::count::ElasticsearchCountRequest;
use crate::elasticsearch::delete_index::ElasticsearchDeleteIndexRequest;
use crate::elasticsearch::expunge_deletes::ElasticsearchExpungeDeletesRequest;
use crate::elasticsearch::get_document::ElasticsearchGetDocumentRequest;
use crate::elasticsearch::get_mapping::ElasticsearchGetMappingRequest;
use crate::elasticsearch::get_settings::ElasticsearchGetSettingsRequest;
use crate::elasticsearch::pg_catalog::ArbitraryRequestType;
use crate::elasticsearch::profile_query::ElasticsearchProfileQueryRequest;
use crate::elasticsearch::put_mapping::ElasticsearchPutMappingRequest;
use crate::elasticsearch::refresh_index::ElasticsearchRefreshIndexRequest;
use crate::elasticsearch::search::ElasticsearchSearchRequest;
use crate::elasticsearch::suggest_term::ElasticsearchSuggestTermRequest;
use crate::elasticsearch::update_settings::ElasticsearchUpdateSettingsRequest;
use crate::executor_manager::get_executor_manager;
use crate::utils::is_nested_field;
use crate::zdbquery::ZDBPreparedQuery;
pub use bulk::*;
pub use create_index::*;
use lazy_static::*;
use pgx::*;
use reqwest::{RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;

lazy_static! {
    static ref NUM_CPUS: usize = num_cpus::get();
}

pub mod pg_catalog {
    use pgx::*;
    use serde::Serialize;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum ArbitraryRequestType {
        GET,
        POST,
        PUT,
        DELETE,
    }
}

#[derive(Clone)]
pub struct Elasticsearch {
    options: ZDBIndexOptions,
}

#[derive(Debug)]
pub struct ElasticsearchError(Option<reqwest::StatusCode>, String);

impl ElasticsearchError {
    pub fn status(&self) -> Option<reqwest::StatusCode> {
        self.0
    }

    pub fn message(&self) -> &str {
        &self.1
    }
}

impl Elasticsearch {
    pub fn new(relation: &PgRelation) -> Self {
        Elasticsearch {
            options: ZDBIndexOptions::from_relation(relation),
        }
    }

    pub fn from_options(options: ZDBIndexOptions) -> Self {
        Elasticsearch { options }
    }

    pub fn index_relation(&self) -> PgRelation {
        self.options.index_relation()
    }

    pub fn heap_relation(&self) -> PgRelation {
        self.options.heap_relation()
    }

    pub fn client() -> reqwest::Client {
        reqwest::ClientBuilder::new()
            .gzip(true)
            .timeout(std::time::Duration::from_secs(3600))
            .build()
            .expect("failed to build reqwest Client")
    }

    pub fn arbitrary_request(
        &self,
        method: ArbitraryRequestType,
        mut endpoint: &str,
        post_data: Option<&'static str>,
    ) -> Result<String, ElasticsearchError> {
        let mut url = String::new();

        if endpoint.starts_with('/') {
            url.push_str(&self.url());
            // strip the leading slash from the endpoint
            // as self.url() is required to have a trailing slash
            endpoint = &endpoint[1..];
        } else {
            url.push_str(&self.base_url());
            url.push('/');
        }

        url.push_str(endpoint);

        let mut builder = match method {
            ArbitraryRequestType::GET => Elasticsearch::client().get(&url),
            ArbitraryRequestType::POST => Elasticsearch::client().post(&url),
            ArbitraryRequestType::PUT => Elasticsearch::client().put(&url),
            ArbitraryRequestType::DELETE => Elasticsearch::client().delete(&url),
        };

        if let Some(post_data) = post_data {
            builder = builder
                .header("content-type", "application/json")
                .body(post_data);
        }

        Elasticsearch::execute_request(builder, |_, body| Ok(body))
    }

    pub fn analyze_text(&self, analyzer: &str, text: &str) -> ElasticsearchAnalyzerRequest {
        ElasticsearchAnalyzerRequest::new_with_text(self, analyzer, text)
    }

    pub fn analyze_with_field(&self, field: &str, text: &str) -> ElasticsearchAnalyzerRequest {
        ElasticsearchAnalyzerRequest::new_with_field(self, field, text)
    }

    pub fn analyze_custom(
        &self,
        field: Option<default!(&str, NULL)>,
        text: Option<default!(&str, NULL)>,
        tokenizer: Option<default!(&str, NULL)>,
        normalizer: Option<default!(&str, NULL)>,
        filter: Option<default!(Array<&str>, NULL)>,
        char_filter: Option<default!(Array<&str>, NULL)>,
    ) -> ElasticsearchAnalyzerRequest {
        ElasticsearchAnalyzerRequest::new_custom(
            self,
            field,
            text,
            tokenizer,
            normalizer,
            filter,
            char_filter,
        )
    }

    pub fn create_index(&self, mapping: Value) -> ElasticsearchCreateIndexRequest {
        ElasticsearchCreateIndexRequest::new(self, mapping)
    }

    pub fn delete_index(&self) -> ElasticsearchDeleteIndexRequest {
        ElasticsearchDeleteIndexRequest::new(self)
    }

    pub fn refresh_index(&self) -> ElasticsearchRefreshIndexRequest {
        ElasticsearchRefreshIndexRequest::new(self)
    }

    pub fn add_alias(&self, alias_name: &str) -> ElasticsearchAliasRequest {
        ElasticsearchAliasRequest::add(self, alias_name)
    }

    pub fn remove_alias(&self, alias_name: &str) -> ElasticsearchAliasRequest {
        ElasticsearchAliasRequest::remove(self, alias_name)
    }

    pub fn expunge_deletes(&self) -> ElasticsearchExpungeDeletesRequest {
        ElasticsearchExpungeDeletesRequest::new(self)
    }

    pub fn update_settings(&self) -> ElasticsearchUpdateSettingsRequest {
        ElasticsearchUpdateSettingsRequest::new(self)
    }

    pub fn put_mapping(&self, mapping: serde_json::Value) -> ElasticsearchPutMappingRequest {
        ElasticsearchPutMappingRequest::new(self, mapping)
    }

    pub fn cat(&self, endpoint: &str) -> ElasticsearchCatRequest {
        ElasticsearchCatRequest::new(self, endpoint)
    }

    pub fn profile_query(&self, query: ZDBPreparedQuery) -> ElasticsearchProfileQueryRequest {
        ElasticsearchProfileQueryRequest::new(self, query)
    }

    pub fn start_bulk(&self) -> ElasticsearchBulkRequest {
        let concurrency = (self.options.shards() as usize)
            .min(NUM_CPUS.min(self.options.bulk_concurrency() as usize));
        ElasticsearchBulkRequest::new(
            self,
            10_000,
            concurrency,
            self.options.batch_size() as usize,
        )
    }

    pub fn open_search(&self, query: ZDBPreparedQuery) -> ElasticsearchSearchRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchSearchRequest::new(self, query)
    }

    pub fn aggregate<T: DeserializeOwned>(
        &self,
        field_name: Option<String>,
        need_filter: bool,
        query: ZDBPreparedQuery,
        agg_request: serde_json::Value,
    ) -> ElasticsearchAggregateSearchRequest<T> {
        let mut aggs = HashMap::new();
        aggs.insert("the_agg".to_string(), agg_request);
        self.aggregate_set(field_name, need_filter, query, aggs)
    }

    pub fn aggregate_set<T: DeserializeOwned>(
        &self,
        field: Option<String>,
        need_filter: bool,
        query: ZDBPreparedQuery,
        aggs: HashMap<String, serde_json::Value>,
    ) -> ElasticsearchAggregateSearchRequest<T> {
        get_executor_manager().wait_for_completion();

        let (is_nested_field, nested_path, filter_query) = {
            if field.is_none() {
                // we don't have a field
                (false, None, None)
            } else {
                let field = field.as_ref().unwrap();
                // maybe it is nested, so lets go look
                let index = PgRelation::with_lock(
                    self.options.oid(),
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                );

                // get the full path, which is the full field name minus the last dotted part
                let mut path = field.rsplitn(2, '.').collect::<Vec<&str>>();
                let path = path.pop().unwrap();

                if is_nested_field(&index, &path) {
                    // is nested, so we also need to generate a filter query for it
                    if need_filter {
                        let mut value = query.query_dsl().clone();
                        let filter_query =
                            ZDBPreparedQuery::extract_nested_filter(path, Some(&mut value));
                        (true, Some(path), filter_query.cloned())
                    } else {
                        (true, Some(path), None)
                    }
                } else {
                    (false, None, None)
                }
            }
        };

        let aggs = aggs
            .into_iter()
            .map(|(k, v)| {
                if is_nested_field {
                    let nested_agg = self.make_nested_agg(&k, v, &nested_path, &filter_query);
                    (k, nested_agg)
                } else {
                    (k, v)
                }
            })
            .collect::<HashMap<String, Value>>();

        ElasticsearchAggregateSearchRequest::new(self, query, aggs)
    }

    pub fn raw_json_aggregate<T: DeserializeOwned>(
        &self,
        agg_request: serde_json::Value,
    ) -> ElasticsearchAggregateSearchRequest<T> {
        get_executor_manager().wait_for_completion();
        ElasticsearchAggregateSearchRequest::from_raw(self, agg_request)
    }

    pub fn count(&self, query: ZDBPreparedQuery) -> ElasticsearchCountRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchCountRequest::new(self, query, false)
    }

    pub fn raw_count<'a>(&self, query: ZDBPreparedQuery) -> ElasticsearchCountRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchCountRequest::new(self, query, true)
    }

    pub fn suggest_terms(
        &self,
        query: ZDBPreparedQuery,
        fieldname: String,
        suggest: String,
    ) -> ElasticsearchSuggestTermRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchSuggestTermRequest::new(self, query, fieldname, suggest)
    }

    pub fn get_document<'a, T: DeserializeOwned>(
        &self,
        id: &'a str,
        realtime: bool,
    ) -> ElasticsearchGetDocumentRequest<'a, T> {
        ElasticsearchGetDocumentRequest::<T>::new(self, id, realtime)
    }

    pub fn get_mapping(&self) -> ElasticsearchGetMappingRequest {
        ElasticsearchGetMappingRequest::new(self)
    }

    pub fn get_settings(&self) -> ElasticsearchGetSettingsRequest {
        ElasticsearchGetSettingsRequest::new(self)
    }

    pub fn url(&self) -> &str {
        self.options.url()
    }

    pub fn base_url(&self) -> String {
        format!("{}{}", self.options.url(), self.options.index_name())
    }

    pub fn alias_url(&self) -> String {
        format!("{}{}", self.options.url(), self.options.alias())
    }

    pub fn index_name(&self) -> &str {
        self.options.index_name()
    }

    pub fn alias_name(&self) -> &str {
        self.options.alias()
    }

    pub fn type_name(&self) -> &str {
        self.options.type_name()
    }

    pub fn execute_request<F, R>(
        builder: RequestBuilder,
        response_parser: F,
    ) -> std::result::Result<R, ElasticsearchError>
    where
        F: FnOnce(reqwest::StatusCode, String) -> std::result::Result<R, ElasticsearchError>,
    {
        match builder.send() {
            // the request was processed by ES, but maybe not successfully
            Ok(mut response) => {
                let code = response.status();
                let mut body_string = String::new();
                response
                    .read_to_string(&mut body_string)
                    .expect("unable to convert HTTP response to a string");

                if code.as_u16() != 200 {
                    // it wasn't a valid response code
                    Err(ElasticsearchError(Some(code), body_string))
                } else {
                    response_parser(code, body_string)
                }
            }

            // the request didn't reach ES
            Err(e) => Err(ElasticsearchError(e.status(), e.to_string())),
        }
    }

    pub fn execute_binary_request<F, R>(
        builder: RequestBuilder,
        response_parser: F,
    ) -> std::result::Result<R, ElasticsearchError>
    where
        F: FnOnce(reqwest::StatusCode, Response) -> std::result::Result<R, ElasticsearchError>,
    {
        match builder.send() {
            // the request was processed by ES, but maybe not successfully
            Ok(mut response) => {
                let code = response.status();

                if code.as_u16() != 200 {
                    let mut buff =
                        Vec::with_capacity(response.content_length().unwrap_or(0x80000) as usize);
                    response
                        .copy_to(&mut buff)
                        .expect("unable to read HTTP response to bytes");

                    let error = serde_cbor::from_slice::<serde_cbor::Value>(&buff)
                        .expect("unable to create error Value from binary response");
                    Err(ElasticsearchError(Some(code), format!("{:?}", error)))
                } else {
                    response_parser(code, response)
                }
            }

            // the request didn't reach ES
            Err(e) => Err(ElasticsearchError(e.status(), e.to_string())),
        }
    }

    pub fn make_nested_agg(
        &self,
        agg_name: &str,
        agg: serde_json::Value,
        path: &Option<&str>,
        filter_query: &Option<serde_json::Value>,
    ) -> serde_json::Value {
        match filter_query {
            Some(filtered_query) => json! {
                {
                    "nested": {
                        "path": path
                    },
                    "aggs": {
                        agg_name: {
                            "filter": filtered_query,
                            "aggs": {
                                agg_name: agg
                            }
                        }
                    }
                }
            },

            None => json! {
                {
                    "nested": {
                        "path": path
                    },
                    "aggs": {
                        agg_name: agg
                    }
                }
            },
        }
    }
}

#[pg_extern(volatile, parallel_safe)]
fn request(
    index: PgRelation,
    endpoint: &str,
    method: default!(ArbitraryRequestType, "'GET'"),
    post_data: Option<default!(&'static str, "NULL")>,
) -> String {
    let es = Elasticsearch::new(&index);
    es.arbitrary_request(method, endpoint, post_data)
        .expect("failed to execute arbitrary request")
}
