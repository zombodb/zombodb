use pgx::stringinfo::StringInfo;
use pgx::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod cast;
mod input;
mod opclass;

extension_sql! {r#"CREATE TYPE pg_catalog.zdbquery;"#}
#[derive(Debug, Serialize, Deserialize)]
pub struct ZDBQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    want_score: Option<()>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_estimate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_json: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_dsl: Option<Value>,
}

impl FromDatum<ZDBQuery> for ZDBQuery {
    #[inline]
    fn from_datum(datum: pg_sys::Datum, is_null: bool, typoid: pg_sys::Oid) -> Option<ZDBQuery> {
        match str::from_datum(datum, is_null, typoid) {
            Some(string) => {
                Some(serde_json::from_str(string).expect("failed to deserialize zdbquery"))
            }
            None => None,
        }
    }
}

impl IntoDatum<ZDBQuery> for ZDBQuery {
    #[inline]
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let string = serde_json::to_string(&self).expect("failed to serialize zdbquery");
        string.into_datum()
    }
}

impl ZDBQuery {
    pub fn new() -> Self {
        ZDBQuery {
            want_score: None,
            row_estimate: None,
            limit: None,
            offset: None,
            min_score: None,
            sort_json: None,
            query_dsl: None,
        }
    }

    pub fn new_with_query_string(query: &str) -> Self {
        let query_json = if query.trim().is_empty() {
            // an empty query means to match everything
            serde_json::json!({"match_all":{}})
        } else {
            // else it gets turned into an Elasticsearch query_string query
            serde_json::json!({"query_string":{"query": query}})
        };

        ZDBQuery {
            want_score: None,
            row_estimate: None,
            limit: None,
            offset: None,
            min_score: None,
            sort_json: None,
            query_dsl: Some(query_json),
        }
    }

    pub fn from_cstr(cstr: &std::ffi::CStr) -> Self {
        ZDBQuery::from_str(cstr.to_str().unwrap())
    }

    pub fn from_str(string: &str) -> Self {
        let value: Value = match serde_json::from_str(string) {
            Ok(value) => value,
            Err(_) => {
                // it's not json so assume it's a query_string
                return ZDBQuery::new_with_query_string(string);
            }
        };

        return match serde_json::from_value::<ZDBQuery>(value.clone()) {
            Ok(mut zdbquery) => {
                if zdbquery.all_none() {
                    zdbquery.query_dsl = Some(value);
                }

                zdbquery
            }
            Err(_) => ZDBQuery::new_with_query_string(string),
        };
    }

    pub fn into_cstr(self) -> &'static std::ffi::CStr {
        let sb = StringInfo::from(serde_json::to_string(&self).unwrap());
        sb.into()
    }

    pub fn all_none(&self) -> bool {
        self.want_score.is_none()
            && self.row_estimate.is_none()
            && self.limit.is_none()
            && self.offset.is_none()
            && self.min_score.is_none()
            && self.sort_json.is_none()
            && self.query_dsl.is_none()
    }

    pub fn want_score(&self) -> bool {
        self.want_score == Some(())
    }

    pub fn set_want_score(&mut self, value: bool) -> &mut Self {
        self.want_score = if value { Some(()) } else { None };
        self
    }

    pub fn row_estimate(&self) -> Option<u64> {
        self.row_estimate
    }

    pub fn set_row_estimate(&mut self, row_estimate: Option<u64>) -> &mut Self {
        self.row_estimate = row_estimate;
        self
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub fn set_limit(&mut self, limit: Option<u64>) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    pub fn set_offset(&mut self, offset: Option<u64>) -> &mut Self {
        self.offset = offset;
        self
    }

    pub fn min_score(&self) -> Option<f64> {
        self.min_score
    }

    pub fn set_min_score(&mut self, min_score: Option<f64>) -> &mut Self {
        self.min_score = min_score;
        self
    }

    pub fn sort_json(&self) -> Option<&Value> {
        self.sort_json.as_ref()
    }

    pub fn set_sort_json(&mut self, sort_json: Option<Value>) -> &mut Self {
        self.sort_json = sort_json;
        self
    }

    pub fn query_dsl(&self) -> Option<&Value> {
        self.query_dsl.as_ref()
    }

    pub fn set_query_dsl(&mut self, query_dsl: Option<Value>) -> &mut Self {
        self.query_dsl = query_dsl;
        self
    }
}
