use pgx::*;
use serde_json::{json, Value};

mod cast;
pub mod mvcc;
mod opclass;

use crate::gucs::ZDB_DEFAULT_ROW_ESTIMATE;
use crate::query_dsl::nested::pg_catalog::ScoreMode;
use crate::zql::ast::{Expr, IndexLink, QualifiedField};
use crate::zql::dsl::expr_to_dsl;
use crate::zql::transformations::field_finder::find_link_for_field;
pub use pg_catalog::*;
use serde::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bool {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must: Option<Vec<ZDBQueryClause>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub should: Option<Vec<ZDBQueryClause>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must_not: Option<Vec<ZDBQueryClause>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Vec<ZDBQueryClause>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConstantScore {
    filter: Box<ZDBQueryClause>,
    boost: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DisMax {
    queries: Vec<ZDBQueryClause>,
    #[serde(skip_serializing_if = "Option::is_none")]
    boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tie_breaker: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Boosting {
    positive: Box<ZDBQueryClause>,
    negative: Box<ZDBQueryClause>,
    #[serde(skip_serializing_if = "Option::is_none")]
    negative_boost: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Nested {
    path: String,
    query: Box<ZDBQueryClause>,
    score_mode: ScoreMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    ignore_unmapped: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZdbQueryString {
    query: String,

    #[serde(flatten)]
    other: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZDBQueryClause {
    #[serde(skip_serializing_if = "Option::is_none")]
    bool: Option<Bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    constant_score: Option<ConstantScore>,

    #[serde(skip_serializing_if = "Option::is_none")]
    dis_max: Option<DisMax>,

    #[serde(skip_serializing_if = "Option::is_none")]
    boosting: Option<Boosting>,

    #[serde(skip_serializing_if = "Option::is_none")]
    nested: Option<Nested>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "query_string")]
    zdb: Option<ZdbQueryString>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    opaque: Option<serde_json::Value>,
}

/// only implemented for testing convenience
#[cfg(any(test, feature = "pg_test"))]
impl PartialEq<Value> for ZDBQueryClause {
    fn eq(&self, other: &Value) -> bool {
        let value =
            serde_json::to_value(self).expect("failed to serialize ZDBQueryClause to Value");
        pgx::log!("left ={}", serde_json::to_string(&value).unwrap());
        pgx::log!("right={}", serde_json::to_string(other).unwrap());
        &value == other
    }
}

#[allow(non_camel_case_types)]
#[pgx_macros::pg_schema]
mod pg_catalog {
    use crate::zdbquery::ZDBQueryClause;
    use crate::zql::ast::IndexLink;
    use pgx::*;
    use serde::*;
    use serde_json::Value;
    use std::collections::HashMap;

    #[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
    #[inoutfuncs]
    pub struct ZDBQuery {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) limit: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) offset: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) min_score: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) sort_json: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) row_estimate: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) query_dsl: Option<ZDBQueryClause>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) want_score: Option<bool>,
        #[serde(skip_serializing_if = "HashMap::is_empty")]
        #[serde(default = "HashMap::new")]
        pub(super) highlights: HashMap<String, Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) link_options: Option<Vec<IndexLink>>,
    }

    #[derive(PostgresEnum, Serialize, Deserialize)]
    pub enum SortDirection {
        asc,
        desc,
    }

    #[derive(PostgresEnum, Serialize, Deserialize)]
    pub enum SortMode {
        min,
        max,
        sum,
        avg,
        median,
    }

    #[derive(PostgresType, Serialize, Deserialize)]
    pub struct SortDescriptorOptions {
        pub(crate) order: SortDirection,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) mode: Option<SortMode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) nested_path: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) nested_filter: Option<ZDBQueryClause>,
    }

    #[derive(PostgresType, Serialize, Deserialize)]
    pub struct SortDescriptor {
        pub(crate) field: String,
        pub(crate) options: SortDescriptorOptions,
    }
}

impl InOutFuncs for ZDBQuery {
    fn input(input: &pgx::cstr_core::CStr) -> Self {
        let input = input.to_str().expect("zdbquery input is not valid UTF8");
        ZDBQuery::from_str(input)
    }

    fn output(&self, buffer: &mut StringInfo)
    where
        Self: serde::ser::Serialize,
    {
        serde_json::to_writer(buffer, &self.as_value())
            .expect("failed to write ZDBQuery to buffer");
    }
}

impl Default for ZDBQuery {
    fn default() -> Self {
        ZDBQuery {
            want_score: None,
            row_estimate: None,
            limit: None,
            offset: None,
            min_score: None,
            sort_json: None,
            query_dsl: None,
            highlights: HashMap::new(),
            link_options: None,
        }
    }
}

#[allow(dead_code)]
impl ZDBQuery {
    pub fn new_with_query_dsl(query_dsl: Value) -> Self {
        ZDBQuery {
            want_score: None,
            row_estimate: None,
            limit: None,
            offset: None,
            min_score: None,
            sort_json: None,
            query_dsl: Some(ZDBQueryClause::opaque(query_dsl)),
            highlights: HashMap::new(),
            link_options: None,
        }
    }

    pub fn new_with_query_clause(clause: ZDBQueryClause) -> Self {
        ZDBQuery {
            want_score: None,
            row_estimate: None,
            limit: None,
            offset: None,
            min_score: None,
            sort_json: None,
            query_dsl: Some(clause),
            highlights: HashMap::new(),
            link_options: None,
        }
    }

    pub fn new_with_query_string(query: &str) -> Self {
        if query.trim().is_empty() {
            // an empty query means to match everything
            return ZDBQuery::new_with_query_dsl(serde_json::json!({"match_all":{}}));
        };

        ZDBQuery {
            want_score: None,
            row_estimate: None,
            limit: None,
            offset: None,
            min_score: None,
            sort_json: None,
            query_dsl: Some(ZDBQueryClause::zdb(query)),
            highlights: HashMap::new(),
            link_options: None,
        }
    }

    pub fn from_str(input: &str) -> Self {
        let value: Value = match serde_json::from_str(input) {
            Ok(value) => value,
            Err(_) => {
                // it's not json so assume it's a query_string
                return ZDBQuery::new_with_query_string(input);
            }
        };

        match serde_json::from_value::<ZDBQuery>(value.clone()) {
            Ok(zdbquery) => {
                if zdbquery.all_none() {
                    // it parsed as valid json but didn't match any of our
                    // struct fields, so treat it as if it's query dsl
                    ZDBQuery::new_with_query_dsl(value)
                } else {
                    // it's a real ZDBQuery
                    zdbquery
                }
            }
            Err(_) => ZDBQuery::new_with_query_string(input),
        }
    }

    pub fn all_none(&self) -> bool {
        self.want_score.is_none()
            && self.row_estimate.is_none()
            && self.limit.is_none()
            && self.offset.is_none()
            && self.min_score.is_none()
            && self.sort_json.is_none()
            && self.query_dsl.is_none()
            && self.highlights.is_empty()
            && self.link_options.is_none()
    }

    pub fn only_query_dsl(&self) -> bool {
        self.query_dsl.is_some()
            && self.want_score.is_none()
            && self.row_estimate.is_none()
            && self.limit.is_none()
            && self.offset.is_none()
            && self.min_score.is_none()
            && self.sort_json.is_none()
            && self.highlights.is_empty()
            && self.link_options.is_none()
    }

    pub fn want_score(&self) -> bool {
        self.want_score == Some(true)
    }

    pub fn set_want_score(mut self, value: bool) -> Self {
        self.want_score = if value { Some(true) } else { None };
        self
    }

    pub fn highlights(&mut self) -> &mut HashMap<String, Value> {
        &mut self.highlights
    }

    pub fn row_estimate(&self) -> i64 {
        match self.row_estimate {
            Some(estimate) => estimate,
            None => ZDB_DEFAULT_ROW_ESTIMATE.get() as i64,
        }
    }

    pub fn set_row_estimate(mut self, row_estimate: Option<i64>) -> Self {
        self.row_estimate = row_estimate;
        self
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub fn set_limit(mut self, limit: Option<u64>) -> Self {
        self.limit = limit;
        self
    }

    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    pub fn set_offset(mut self, offset: Option<u64>) -> Self {
        self.offset = offset;
        self
    }

    pub fn min_score(&self) -> Option<f64> {
        self.min_score
    }

    pub fn set_min_score(mut self, min_score: Option<f64>) -> Self {
        self.min_score = min_score;
        self
    }

    pub fn sort_json(&self) -> Option<&Value> {
        self.sort_json.as_ref()
    }

    pub fn set_sort_descriptors<I>(mut self, descriptors: I) -> Self
    where
        I: IntoIterator<Item = Option<SortDescriptor>>,
    {
        // collect all non-None (NULL) sort descriptors
        let mut v = Vec::new();
        for descriptor in descriptors {
            if let Some(descriptor) = descriptor {
                v.push(json! {
                    {
                        &descriptor.field: descriptor.options
                    }
                });
            }
        }

        if !v.is_empty() {
            // we have at least one, so serialize it
            self.sort_json = Some(serde_json::to_value(v).unwrap());
        } else {
            // else, we don't have any
            self.sort_json = None;
        }
        self
    }

    pub fn set_sort_json(mut self, sort_json: Option<Value>) -> Self {
        self.sort_json = sort_json;
        self
    }

    pub fn set_query_dsl(mut self, query_dsl: Option<ZDBQueryClause>) -> Self {
        self.query_dsl = query_dsl;
        self
    }

    pub fn link_options(&self) -> Option<Vec<IndexLink>> {
        self.link_options.clone()
    }

    pub fn set_link_options(mut self, links: Vec<IndexLink>) -> Self {
        self.link_options = if links.is_empty() { None } else { Some(links) };
        self
    }

    pub fn query_dsl(&self) -> ZDBQueryClause {
        self.query_dsl
            .as_ref()
            .cloned()
            .expect("ZDBQuery does not contain query dsl")
    }

    /// Convert the this `ZDBQuery` into a `serde_json::Value` using the most minimal form we can
    pub fn into_value(self) -> serde_json::Value {
        self.as_value()
    }

    /// Return this `ZDBQuery` as an owned `serde_json::Value` using the most minimal form we can
    fn as_value(&self) -> serde_json::Value {
        if self.only_query_dsl() {
            serde_json::to_value(&self.query_dsl).expect("failed to serialize to json")
        } else {
            serde_json::to_value(&self).expect("failed to serialize to json")
        }
    }

    pub fn prepare(
        self,
        index: &PgRelation,
        field_name: Option<String>,
    ) -> (ZDBPreparedQuery, PgRelation) {
        match field_name {
            Some(field_name) => {
                let indexes = self
                    .link_options()
                    .unwrap_or_else(|| IndexLink::from_zdb(&index));
                let target_link = find_link_for_field(
                    &QualifiedField {
                        index: None,
                        field: field_name.into(),
                    },
                    &IndexLink::from_relation(index),
                    &indexes,
                );
                let target_index = if let Some(target_link) = target_link.as_ref() {
                    target_link.open_index().expect("failed to open index")
                } else {
                    index.clone()
                };
                (
                    self.prepare_with_target(&target_index, target_link),
                    target_index,
                )
            }
            None => (self.prepare_with_target(index, None), index.clone()),
        }
    }

    fn prepare_with_target(
        mut self,
        index: &PgRelation,
        target_link: Option<IndexLink>,
    ) -> ZDBPreparedQuery {
        let index_links = self
            .link_options()
            .unwrap_or_else(|| IndexLink::from_zdb(index));
        self.rewrite(index, &index_links, target_link);
        let json = serde_json::to_value(&self.query_dsl)
            .expect("failed to convert ZDBQuery to a json Value");
        ZDBPreparedQuery(self, json)
    }

    fn rewrite(
        &mut self,
        index: &PgRelation,
        index_links: &Vec<IndexLink>,
        target_link: Option<IndexLink>,
    ) {
        ZDBQuery::rewrite_zdb_query_clause(
            self.query_dsl
                .as_mut()
                .expect("ZDBQuery does not contain query dsl"),
            index_links,
            index,
            &IndexLink::from_relation(&index),
            &target_link,
        )
    }

    fn rewrite_zdb_query_clause(
        clause: &mut ZDBQueryClause,
        index_links: &Vec<IndexLink>,
        index: &PgRelation,
        root_link: &IndexLink,
        target_link: &Option<IndexLink>,
    ) {
        if clause.zdb.is_some() {
            if let Some(zdb) = clause.zdb.as_ref() {
                if !zdb.other.is_empty() {
                    // the ZdbQueryString also has other properties, so it's really an ES query_string
                    // as such, there's nothing for us to do
                    return;
                }
                let mut used_fields = HashSet::new();
                let expr = Expr::from_str(
                    &index,
                    "zdb_all",
                    &zdb.query,
                    index_links,
                    target_link,
                    &mut used_fields,
                )
                .expect("failed to parse query");
                let parsed = expr_to_dsl(&root_link, index_links, &expr);

                clause.zdb = None;
                clause.opaque = Some(parsed);
            }
        } else if let Some(bool) = &mut clause.bool {
            if bool.must.is_some() {
                bool.must.as_mut().unwrap().iter_mut().for_each(|c| {
                    ZDBQuery::rewrite_zdb_query_clause(
                        c,
                        index_links,
                        index,
                        root_link,
                        target_link,
                    );
                });
            }

            if bool.should.is_some() {
                bool.should.as_mut().unwrap().iter_mut().for_each(|c| {
                    ZDBQuery::rewrite_zdb_query_clause(
                        c,
                        index_links,
                        index,
                        root_link,
                        target_link,
                    );
                });
            }

            if bool.must_not.is_some() {
                bool.must_not.as_mut().unwrap().iter_mut().for_each(|c| {
                    ZDBQuery::rewrite_zdb_query_clause(
                        c,
                        index_links,
                        index,
                        root_link,
                        target_link,
                    );
                });
            }

            if bool.filter.is_some() {
                bool.filter.as_mut().unwrap().iter_mut().for_each(|c| {
                    ZDBQuery::rewrite_zdb_query_clause(
                        c,
                        index_links,
                        index,
                        root_link,
                        target_link,
                    );
                });
            }
        } else if let Some(nested) = &mut clause.nested {
            ZDBQuery::rewrite_zdb_query_clause(
                nested.query.as_mut(),
                index_links,
                index,
                root_link,
                target_link,
            );
        } else if let Some(boosting) = &mut clause.boosting {
            ZDBQuery::rewrite_zdb_query_clause(
                boosting.positive.as_mut(),
                index_links,
                index,
                root_link,
                target_link,
            );
            ZDBQuery::rewrite_zdb_query_clause(
                boosting.negative.as_mut(),
                index_links,
                index,
                root_link,
                target_link,
            );
        } else if let Some(dis_max) = &mut clause.dis_max {
            dis_max.queries.iter_mut().for_each(|c| {
                ZDBQuery::rewrite_zdb_query_clause(c, index_links, index, root_link, target_link);
            });
        } else if let Some(constant_score) = &mut clause.constant_score {
            ZDBQuery::rewrite_zdb_query_clause(
                constant_score.filter.as_mut(),
                index_links,
                index,
                root_link,
                target_link,
            );
        }
    }
}

impl ZDBQueryClause {
    pub fn opaque(json: serde_json::Value) -> Self {
        ZDBQueryClause {
            bool: None,
            constant_score: None,
            dis_max: None,
            boosting: None,
            nested: None,
            zdb: None,
            opaque: Some(json),
        }
    }

    pub fn zdb(query: &str) -> Self {
        ZDBQueryClause {
            bool: None,
            constant_score: None,
            dis_max: None,
            boosting: None,
            nested: None,
            zdb: Some(ZdbQueryString {
                query: query.into(),
                other: HashMap::new(),
            }),
            opaque: None,
        }
    }

    pub fn bool(
        must: Option<Vec<ZDBQueryClause>>,
        should: Option<Vec<ZDBQueryClause>>,
        must_not: Option<Vec<ZDBQueryClause>>,
        filter: Option<Vec<ZDBQueryClause>>,
    ) -> Self {
        ZDBQueryClause {
            bool: Some(Bool {
                must,
                should,
                must_not,
                filter,
            }),
            constant_score: None,
            dis_max: None,
            boosting: None,
            nested: None,
            zdb: None,
            opaque: None,
        }
    }

    pub fn into_bool(self) -> Option<Bool> {
        self.bool
    }

    pub fn nested(
        path: String,
        query: ZDBQueryClause,
        score_mode: ScoreMode,
        ignore_unmapped: Option<bool>,
    ) -> Self {
        ZDBQueryClause {
            bool: None,
            constant_score: None,
            dis_max: None,
            boosting: None,
            nested: Some(Nested {
                path,
                query: Box::new(query),
                score_mode,
                ignore_unmapped,
            }),
            zdb: None,
            opaque: None,
        }
    }

    pub fn constant_score(filter: ZDBQueryClause, boost: f32) -> Self {
        ZDBQueryClause {
            bool: None,
            constant_score: Some(ConstantScore {
                filter: Box::new(filter),
                boost,
            }),
            dis_max: None,
            boosting: None,
            nested: None,
            zdb: None,
            opaque: None,
        }
    }

    pub fn dis_max(
        queries: Vec<ZDBQueryClause>,
        boost: Option<f32>,
        tie_breaker: Option<f32>,
    ) -> Self {
        ZDBQueryClause {
            bool: None,
            constant_score: None,
            dis_max: Some(DisMax {
                queries,
                boost,
                tie_breaker,
            }),
            boosting: None,
            nested: None,
            zdb: None,
            opaque: None,
        }
    }

    pub fn boosting(
        positive_query: ZDBQueryClause,
        negative_query: ZDBQueryClause,
        negative_boost: Option<f32>,
    ) -> Self {
        ZDBQueryClause {
            bool: None,
            constant_score: None,
            dis_max: None,
            boosting: Some(Boosting {
                positive: Box::new(positive_query),
                negative: Box::new(negative_query),
                negative_boost,
            }),
            nested: None,
            zdb: None,
            opaque: None,
        }
    }
}

#[derive(Debug)]
pub struct ZDBPreparedQuery(ZDBQuery, serde_json::Value);

impl ZDBPreparedQuery {
    pub fn query_dsl(&self) -> &serde_json::Value {
        &self.1
    }

    pub fn take_query_dsl(self) -> serde_json::Value {
        self.1
    }

    pub fn limit(&self) -> Option<u64> {
        self.0.limit
    }

    pub fn offset(&self) -> Option<u64> {
        self.0.offset
    }

    pub fn min_score(&self) -> Option<f64> {
        self.0.min_score
    }

    pub fn sort_json(&self) -> Option<&serde_json::Value> {
        self.0.sort_json()
    }

    pub fn want_score(&self) -> bool {
        self.0.want_score.unwrap_or_default()
    }

    pub fn has_highlights(&self) -> bool {
        !self.0.highlights.is_empty()
    }

    pub fn highlights(&self) -> &HashMap<String, serde_json::Value> {
        &self.0.highlights
    }

    pub fn extract_nested_filter<'a>(
        path: &str,
        query: Option<&'a mut serde_json::Value>,
    ) -> Option<&'a mut serde_json::Value> {
        if query.is_none() {
            return None;
        }
        let query = query.unwrap();
        let clause_string = serde_json::to_string(query).unwrap();
        if !clause_string.contains(&format!("\"{}.", path)) {
            // no query against the field from this level down
            return None;
        }

        match query {
            Value::Object(obj) => {
                let mut to_remove = HashSet::new();
                let mut nested_replacement = None;
                for (k, v) in obj.iter_mut() {
                    if k == "bool"
                        || k == "must"
                        || k == "must_not"
                        || k == "should"
                        || k == "filter"
                        || k == "constant_score"
                        || k == "boosting"
                        || k == "dis_max"
                    {
                        // inspect this value to see if we should remove it or not
                        if ZDBPreparedQuery::extract_nested_filter(path, Some(v)).is_none() {
                            to_remove.insert(k.clone());
                        }
                    } else if k == "nested" {
                        // first we don't want to keep the "nested" node
                        to_remove.insert(k.clone());

                        // instead, we want to pull up the top-level object in the "nested" node's "query" property
                        let nested_query = v.as_object_mut().unwrap().remove("query").unwrap();
                        let as_map = nested_query.as_object().unwrap();

                        // and whatever that is, its kEY and vALUE become the thing we'll add back into the map
                        let (k, v) = as_map.iter().next().unwrap();
                        nested_replacement = Some((k.clone(), v.clone()));
                    } else {
                        // not a thing we care to keep for the filter
                        to_remove.insert(k.clone());
                    }
                }

                // now remove all the keys we decided
                for k in to_remove.into_iter() {
                    obj.remove(&k);
                }

                // if we have a replacement for a "nested" node, go ahead and do that
                if let Some((k, v)) = nested_replacement {
                    if k == "nested" {
                        let mut value = json! { { k : v } };
                        let nested_filter =
                            ZDBPreparedQuery::extract_nested_filter(path, Some(&mut value));
                        if nested_filter.is_some() {
                            let (k, v) = value.as_object().unwrap().iter().next().unwrap();
                            obj.insert(k.clone(), v.clone());
                        }
                    } else {
                        obj.insert(k, v);
                    }
                }

                if obj.is_empty() {
                    return None;
                }
            }

            Value::Array(v) => {
                let mut i = 0;
                while i < v.len() {
                    match v.get_mut(i) {
                        Some(value) => {
                            if ZDBPreparedQuery::extract_nested_filter(path, Some(value)).is_none()
                            {
                                // we can remove this element b/c it's empty
                                v.remove(i);
                            } else {
                                // move on to the next node
                                i += 1;
                            }
                        }
                        None => break,
                    }
                }

                if v.is_empty() {
                    return None;
                }
            }

            _ => {
                return None;
            }
        }

        Some(query)
    }
}

#[pg_extern(immutable, parallel_safe)]
fn to_query_dsl(query: ZDBQuery) -> Option<Json> {
    Some(Json(
        serde_json::to_value(query.query_dsl()).expect("failed to convert query to JSON"),
    ))
}

#[pg_extern(immutable, parallel_safe)]
fn to_queries_dsl(queries: Array<ZDBQuery>) -> Vec<Option<Json>> {
    let mut result = Vec::new();
    for query in queries.iter() {
        match query {
            Some(query) => result.push(to_query_dsl(query)),
            None => result.push(None),
        }
    }

    result
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use crate::zdbquery::*;
    use serde_json::json;

    #[pg_test]
    fn test_zdbquery_in_with_query_string() {
        let input = pgx::cstr_core::CStr::from_bytes_with_nul(b"this is a test\0").unwrap();
        let zdbquery = pg_catalog::zdbquery_in(input);
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(
            json,
            json!( {"query_dsl":{"query_string":{"query":"this is a test"}}} )
        );
    }

    #[pg_test]
    fn test_zdbquery_in_with_query_dsl() {
        let input = pgx::cstr_core::CStr::from_bytes_with_nul(b" {\"match_all\":{}} \0").unwrap();
        let zdbquery = pg_catalog::zdbquery_in(input);
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(json, json!( {"query_dsl":{"match_all":{}}} ));
    }

    #[pg_test]
    fn test_zdbquery_in_with_full_query() {
        let input = pgx::cstr_core::CStr::from_bytes_with_nul(
            b" {\"query_dsl\":{\"query_string\":{\"query\":\"this is a test\"}}} \0",
        )
        .unwrap();
        let zdbquery = pg_catalog::zdbquery_in(input);
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(
            json,
            json!( {"query_dsl":{"query_string":{"query":"this is a test"}}} )
        );
    }
}
