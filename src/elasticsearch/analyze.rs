use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use pgx::{prelude::*, *};
use serde::*;
use serde_json::*;

#[derive(Deserialize, Serialize)]
pub struct AnalyzedData {
    pub tokens: Vec<Token>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Token {
    #[serde(rename = "type")]
    pub type_: String,
    pub token: String,
    pub position: i32,
    pub start_offset: i64,
    pub end_offset: i64,
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
    pub fn execute(self) -> std::result::Result<AnalyzedData, ElasticsearchError> {
        let client = Elasticsearch::client().post(&self.url);

        Elasticsearch::execute_json_request(client, Some(self.analyze_json), |body| {
            Ok(serde_json::from_reader(body).unwrap())
        })
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
            url: format!("{}/_analyze", elasticsearch.base_url()),
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
        field: Option<&str>,
        text: Option<&str>,
        tokenizer: Option<&str>,
        normalizer: Option<&str>,
        filter: Option<Array<&str>>,
        char_filter: Option<Array<&str>>,
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
) -> TableIterator<
    'static,
    (
        name!(type, String),
        name!(token, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    let elasticsearch = Elasticsearch::new(&index);

    elasticsearch_request_return(elasticsearch.analyze_text(analyzer, text))
}

#[pg_extern(immutable, parallel_safe)]
pub(crate) fn analyze_with_field(
    index: PgRelation,
    field: &str,
    text: &str,
) -> TableIterator<
    'static,
    (
        name!(type, String),
        name!(token, String),
        name!(position, i32),
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
    field: default!(Option<&str>, NULL),
    text: default!(Option<&str>, NULL),
    tokenizer: default!(Option<&str>, NULL),
    normalizer: default!(Option<&str>, NULL),
    filter: default!(Option<Array<&str>>, NULL),
    char_filter: default!(Option<Array<&str>>, NULL),
) -> TableIterator<
    'static,
    (
        name!(type, String),
        name!(token, String),
        name!(position, i32),
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
) -> TableIterator<
    'static,
    (
        name!(type, String),
        name!(token, String),
        name!(position, i32),
        name!(start_offset, i64),
        name!(end_offset, i64),
    ),
> {
    TableIterator::new(
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
            }),
    )
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use pgx::*;

    #[pg_test]
    #[initialize(es = true)]
    fn test_analyze_text() {
        Spi::run("CREATE TABLE test_analyze_text AS SELECT * FROM generate_series(1, 100);");
        Spi::run("CREATE INDEX idxtest_analyze_text ON test_analyze_text USING zombodb ((test_analyze_text.*));");
        Spi::connect(|client| {
            let table = client.select(
                " SELECT * FROM zdb.analyze_text('idxtest_analyze_text', 'standard', 'this is a test');",
                None,
                None,
            );

            //    type        | token | position | start_offset | end_offset
            //    ------------+-------+----------+--------------+------------
            //     <ALPHANUM> | this  |        0 |            0 |          4
            //     <ALPHANUM> | is    |        1 |            5 |          7
            //     <ALPHANUM> | a     |        2 |            8 |          9
            //     <ALPHANUM> | test  |        3 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 0, 0, 4),
                ("<ALPHANUM>", "is", 1, 5, 7),
                ("<ALPHANUM>", "a", 2, 8, 9),
                ("<ALPHANUM>", "test", 3, 10, 14),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_analyze_with_field() {
        Spi::run("CREATE TABLE test_analyze_with_field AS SELECT * FROM generate_series(1, 100);");
        Spi::run("CREATE INDEX idxtest_analyze_with_field ON test_analyze_with_field USING zombodb ((test_analyze_with_field.*));");
        Spi::connect(|client| {
            let table = client.select(
                " SELECT * FROM zdb.analyze_with_field('idxtest_analyze_with_field', 'column', 'this is a test');",
                None,
                None,
            );

            //    type        | token | position | start_offset | end_offset
            //    ------------+-------+----------+--------------+------------
            //     <ALPHANUM> | this  |        0 |            0 |          4
            //     <ALPHANUM> | is    |        1 |            5 |          7
            //     <ALPHANUM> | a     |        2 |            8 |          9
            //     <ALPHANUM> | test  |        3 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 0, 0, 4),
                ("<ALPHANUM>", "is", 1, 5, 7),
                ("<ALPHANUM>", "a", 2, 8, 9),
                ("<ALPHANUM>", "test", 3, 10, 14),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_analyze_custom() {
        Spi::run("CREATE TABLE test_analyze_custom AS SELECT * FROM generate_series(1, 100);");
        Spi::run("CREATE INDEX idxtest_analyze_custom ON test_analyze_custom USING zombodb ((test_analyze_custom.*));");
        Spi::connect(|client| {
            let table = client.select(
                " SELECT * FROM zdb.analyze_custom(index=>'idxtest_analyze_custom',text=> 'this is a test', tokenizer =>'standard');",
                None,
                None,
            );

            //    type        | token | position | start_offset | end_offset
            //    ------------+-------+----------+--------------+------------
            //     <ALPHANUM> | this  |        0 |            0 |          4
            //     <ALPHANUM> | is    |        1 |            5 |          7
            //     <ALPHANUM> | a     |        2 |            8 |          9
            //     <ALPHANUM> | test  |        3 |           10 |         14
            let expect = vec![
                ("<ALPHANUM>", "this", 0, 0, 4),
                ("<ALPHANUM>", "is", 1, 5, 7),
                ("<ALPHANUM>", "a", 2, 8, 9),
                ("<ALPHANUM>", "test", 3, 10, 14),
            ];

            test_table(table, expect);

            Ok(Some(()))
        });
    }

    fn test_table(mut table: SpiTupleTable, expect: Vec<(&str, &str, i32, i64, i64)>) {
        let mut i = 0;
        while let Some(_) = table.next() {
            let ttype = table.get_datum::<&str>(1).unwrap();
            let token = table.get_datum::<&str>(2).unwrap();
            let pos = table.get_datum::<i32>(3).unwrap();
            let start_offset = table.get_datum::<i64>(4).unwrap();
            let end_offset = table.get_datum::<i64>(5).unwrap();

            let row_tuple = (ttype, token, pos, start_offset, end_offset);

            assert_eq!(expect[i], row_tuple);

            i += 1;
        }
        assert_eq!(expect.len(), i);
    }
}
