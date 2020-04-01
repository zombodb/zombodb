use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Deserialize, Serialize)]
pub struct AnalyzedData {
    tokens: Vec<Token>,
}

#[derive(Deserialize, Serialize)]
pub struct Token {
    #[serde(rename = "type")]
    type_: String,
    token: String,
    position: i64,
    start_offset: i64,
    end_offset: i64,
}

pub struct ElasticsearchAnalyzerRequest {
    elasticsearch: Elasticsearch,
    analyze_json: serde_json::Value,
    url: String,
}

#[inline]
fn make_custom_analyzer_request(custom: Custom) -> serde_json::Value {
    json! {
            custom
    }
}

#[derive(Serialize)]
struct Custom<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokenizer: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    normalizer: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<Array<'a, &'a str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    char_filter: Option<Array<'a, &'a str>>,
}

impl ElasticsearchAnalyzerRequest {
    pub fn execute(&self) -> std::result::Result<AnalyzedData, ElasticsearchError> {
        let client = reqwest::Client::new()
            .post(&self.url)
            .header("content-type", "application/json")
            .body(serde_json::to_string(&self.analyze_json).unwrap());

        Elasticsearch::execute_request(client, |_, body| Ok(serde_json::from_str(&body).unwrap()))
    }

    pub fn new_with_text(
        elasticsearch: &Elasticsearch,
        analyze_type: &str,
        analyze_text: &str,
    ) -> ElasticsearchAnalyzerRequest {
        ElasticsearchAnalyzerRequest {
            elasticsearch: elasticsearch.clone(),
            analyze_json: json!(
               {
                "analyzer": analyze_type,
                "text":analyze_text
                }
            ),
            url: format!("{}_analyze", elasticsearch.url()),
        }
    }

    pub fn new_with_field(
        elasticsearch: &Elasticsearch,
        analyze_field: &str,
        analyze_text: &str,
    ) -> ElasticsearchAnalyzerRequest {
        ElasticsearchAnalyzerRequest {
            elasticsearch: elasticsearch.clone(),
            analyze_json: json!(
               {
                "field": analyze_field,
                "text":analyze_text
                }
            ),
            url: format!("{}/_analyze", elasticsearch.base_url()),
        }
    }

    pub fn new_custom(
        elasticsearch: &Elasticsearch,
        field: Option<default!(&str, NULL)>,
        text: Option<default!(&str, NULL)>,
        tokenizer: Option<default!(&str, NULL)>,
        normalizer: Option<default!(&str, NULL)>,
        filter: Option<default!(Array<&str>, NULL)>,
        char_filter: Option<default!(Array<&str>, NULL)>,
    ) -> ElasticsearchAnalyzerRequest {
        let custom = Custom {
            field,
            text,
            tokenizer,
            normalizer,
            filter,
            char_filter,
        };
        ElasticsearchAnalyzerRequest {
            elasticsearch: elasticsearch.clone(),
            analyze_json: make_custom_analyzer_request(custom),
            url: format!("{}_analyze", elasticsearch.url()),
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
fn analyze_text(
    index: PgRelation,
    analyzer: &str,
    text: &str,
) -> impl std::iter::Iterator<
    Item = (
        name!(type, String),
        name!(token, String),
        name!(position, i64),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let elasticsearch = Elasticsearch::new(&index);

    elasticsearch_request_return(elasticsearch.analyze_text(analyzer, text))
}

#[pg_extern(immutable, parallel_safe)]
fn analyze_with_field(
    index: PgRelation,
    field: &str,
    text: &str,
) -> impl std::iter::Iterator<
    Item = (
        name!(type, String),
        name!(token, String),
        name!(position, i64),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let elasticsearch = Elasticsearch::new(&index);

    elasticsearch_request_return(elasticsearch.analyze_with_field(field, text))
}

#[pg_extern(immutable, parallel_safe)]
fn analyze_custom(
    index: PgRelation,
    field: Option<default!(&str, NULL)>,
    text: Option<default!(&str, NULL)>,
    tokenizer: Option<default!(&str, NULL)>,
    normalizer: Option<default!(&str, NULL)>,
    filter: Option<default!(Array<&str>, NULL)>,
    char_filter: Option<default!(Array<&str>, NULL)>,
) -> impl std::iter::Iterator<
    Item = (
        name!(type, String),
        name!(token, String),
        name!(position, i64),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let elasticsearch = Elasticsearch::new(&index);

    elasticsearch_request_return(elasticsearch.analyze_custom(
        field,
        text,
        tokenizer,
        normalizer,
        filter,
        char_filter,
    ))
}

fn elasticsearch_request_return(
    request: ElasticsearchAnalyzerRequest,
) -> impl std::iter::Iterator<
    Item = (
        name!(type, String),
        name!(token, String),
        name!(position, i64),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    request
        .execute()
        .expect("failed to execute Analyze search")
        .tokens
        .into_iter()
        .map(|entry| {
            (
                entry.type_,
                entry.token,
                entry.position,
                entry.start_offset,
                entry.end_offset,
            )
        })
}
