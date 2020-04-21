#![allow(dead_code)]

mod aggregates;
pub(crate) mod analyze;
mod bulk;
mod cat;
mod count;
mod create_index;
mod delete_index;
mod expunge_deletes;
mod get_document;
mod get_mapping;
mod profile_query;
mod refresh_index;
mod update_settings;

pub mod aggregate_search;
pub mod search;

use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::aggregate_search::ElasticsearchAggregateSearchRequest;
use crate::elasticsearch::analyze::ElasticsearchAnalyzerRequest;
use crate::elasticsearch::cat::ElasticsearchCatRequest;
use crate::elasticsearch::count::ElasticsearchCountRequest;
use crate::elasticsearch::delete_index::ElasticsearchDeleteIndexRequest;
use crate::elasticsearch::expunge_deletes::ElasticsearchExpungeDeletesRequest;
use crate::elasticsearch::get_document::ElasticsearchGetDocumentRequest;
use crate::elasticsearch::get_mapping::ElasticsearchGetMappingRequest;
use crate::elasticsearch::pg_catalog::ArbitraryRequestType;
use crate::elasticsearch::profile_query::ElasticsearchProfileQueryRequest;
use crate::elasticsearch::refresh_index::ElasticsearchRefreshIndexRequest;
use crate::elasticsearch::search::ElasticsearchSearchRequest;
use crate::elasticsearch::update_settings::ElasticsearchUpdateSettingsRequest;
use crate::executor_manager::get_executor_manager;
use crate::zdbquery::ZDBQuery;
pub use bulk::*;
pub use create_index::*;
use lazy_static::*;
use pgx::*;
use reqwest::RequestBuilder;
use serde::de::DeserializeOwned;
use serde_json::Value;
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
    pub fn new(indexrel: &PgRelation) -> Self {
        Elasticsearch {
            options: ZDBIndexOptions::from(indexrel),
        }
    }

    pub fn from_options(options: ZDBIndexOptions) -> Self {
        Elasticsearch { options }
    }

    pub fn arbitrary_request(
        &self,
        method: ArbitraryRequestType,
        endpoint: &str,
        post_data: Option<&'static str>,
    ) -> Result<String, ElasticsearchError> {
        let mut url = String::new();

        if endpoint.starts_with('/') {
            url.push_str(&self.url());
        } else {
            url.push_str(&self.base_url());
            url.push('/');
        }

        url.push_str(endpoint);

        let mut builder = match method {
            ArbitraryRequestType::GET => reqwest::Client::new().get(&url),
            ArbitraryRequestType::POST => reqwest::Client::new().post(&url),
            ArbitraryRequestType::PUT => reqwest::Client::new().put(&url),
            ArbitraryRequestType::DELETE => reqwest::Client::new().delete(&url),
        };

        if post_data.is_some() {
            builder = builder
                .header("content-type", "application/json")
                .body(post_data.unwrap());
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

    pub fn expunge_deletes(&self) -> ElasticsearchExpungeDeletesRequest {
        ElasticsearchExpungeDeletesRequest::new(self)
    }

    pub fn update_settings(&self, old_alias: Option<&str>) -> ElasticsearchUpdateSettingsRequest {
        ElasticsearchUpdateSettingsRequest::new(self, old_alias)
    }

    pub fn cat(&self, endpoint: &str) -> ElasticsearchCatRequest {
        ElasticsearchCatRequest::new(self, endpoint)
    }

    pub fn profile_query(&self, query: ZDBQuery) -> ElasticsearchProfileQueryRequest {
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

    pub fn open_search(&self, query: ZDBQuery) -> ElasticsearchSearchRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchSearchRequest::new(self, query)
    }

    pub fn aggregate<T: DeserializeOwned>(
        &self,
        query: ZDBQuery,
        agg_request: serde_json::Value,
    ) -> ElasticsearchAggregateSearchRequest<T> {
        get_executor_manager().wait_for_completion();
        ElasticsearchAggregateSearchRequest::new(self, query, agg_request)
    }

    pub fn raw_json_aggregate<T: DeserializeOwned>(
        &self,
        agg_request: serde_json::Value,
    ) -> ElasticsearchAggregateSearchRequest<T> {
        get_executor_manager().wait_for_completion();
        ElasticsearchAggregateSearchRequest::from_raw(self, agg_request)
    }

    pub fn count(&self, query: ZDBQuery) -> ElasticsearchCountRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchCountRequest::new(self, query, false)
    }

    pub fn raw_count(&self, query: ZDBQuery) -> ElasticsearchCountRequest {
        get_executor_manager().wait_for_completion();
        ElasticsearchCountRequest::new(self, query, true)
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

    pub fn url(&self) -> &str {
        self.options.url()
    }

    pub fn base_url(&self) -> String {
        format!("{}{}", self.options.url(), self.options.index_name())
    }

    pub fn index_name(&self) -> &str {
        self.options.index_name()
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
}

#[pg_extern(immutable, parallel_safe)]
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
