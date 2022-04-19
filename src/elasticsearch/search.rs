use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::gucs::ZDB_ACCELERATOR;
use crate::utils::read_vlong;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBPreparedQuery;
use pgx::PgBuiltInOids;
use serde::*;
use serde_json::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const SEARCH_FILTER_PATH:&str = "_scroll_id,_shards.*,hits.total,hits.max_score,hits.hits._score,hits.hits.fields.*,hits.hits.highlight.*";
const SEARCH_FILTER_PATH_NO_SCORE: &str =
    "_scroll_id,_shards.*,hits.total,hits.hits.fields.*,hits.hits.highlight.*";

pub struct ElasticsearchSearchRequest {
    elasticsearch: Elasticsearch,
    query: ZDBPreparedQuery,
}

#[derive(Debug, Deserialize)]
pub struct HitsTotal {
    value: u64,
}

#[derive(Deserialize, Debug)]
pub struct Fields {
    pub zdb_ctid: Option<[u64; 1]>,
    pub zdb_xmin: Option<[u64; 1]>,
    pub zdb_xmax: Option<[u64; 1]>,
}

impl Default for Fields {
    fn default() -> Self {
        Fields {
            zdb_ctid: None,
            zdb_xmin: None,
            zdb_xmax: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct InnerHit {
    #[serde(rename = "_score")]
    score: Option<f64>,
    fields: Option<Fields>,
    highlight: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Deserialize)]
pub struct Hits {
    total: HitsTotal,
    hits: Option<Vec<InnerHit>>,
}

#[derive(Debug, Deserialize)]
pub struct Shards {
    total: usize,
    successful: usize,
    skipped: usize,
    failed: usize,
    failures: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ElasticsearchSearchResponse {
    #[serde(skip)]
    elasticsearch: Option<Elasticsearch>,
    #[serde(skip)]
    limit: Option<u64>,
    #[serde(skip)]
    offset: Option<u64>,
    #[serde(skip)]
    track_scores: bool,
    #[serde(skip)]
    should_sort_hits: bool,

    #[serde(rename = "_scroll_id")]
    scroll_id: Option<String>,
    #[serde(rename = "_shards")]
    shards: Option<Shards>,
    hits: Option<Hits>,

    #[serde(skip)]
    fast_terms: Option<Vec<u64>>,
}

impl InnerHit {
    #[inline]
    fn into_tuple(
        self,
    ) -> Option<(
        f64,
        u64,
        Option<Fields>,
        Option<HashMap<String, Vec<String>>>,
    )> {
        let fields = self.fields.unwrap_or_default();
        let score = self.score.unwrap_or_default();
        let highlight = self.highlight;
        let ctid = fields.zdb_ctid.map_or(0, |v| v[0]);

        if ctid == 0 {
            // this most likely represents the "zdb_aborted_xids" document,
            // so we can just skip it
            None
        } else {
            Some((score, ctid, Some(fields), highlight))
        }
    }
}

impl ElasticsearchSearchRequest {
    pub fn new(elasticsearch: &Elasticsearch, query: ZDBPreparedQuery) -> Self {
        ElasticsearchSearchRequest {
            elasticsearch: elasticsearch.clone(),
            query,
        }
    }

    pub fn execute(self) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        ElasticsearchSearchRequest::initial_search(&self.elasticsearch, self.query, None)
    }

    pub fn execute_with_fields(
        self,
        extra_fields: Vec<&str>,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        ElasticsearchSearchRequest::initial_search(
            &self.elasticsearch,
            self.query,
            Some(extra_fields),
        )
    }

    fn initial_search(
        elasticsearch: &Elasticsearch,
        query: ZDBPreparedQuery,
        extra_fields: Option<Vec<&str>>,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        let mut should_sort_hits = false;
        let mut url = String::new();
        url.push_str(&elasticsearch.base_url());
        url.push_str("/_search");
        url.push_str("?search_type=query_then_fetch");
        url.push_str("&_source=false");
        url.push_str("&scroll=5s");
        url.push_str("&stored_fields=_none_");

        // we always want the zdb_ctid field
        let have_extra_fields = extra_fields.is_some();
        let mut docvalue_fields = extra_fields.unwrap_or_default();
        docvalue_fields.push("zdb_ctid");
        url.push_str(&format!("&docvalue_fields={}", docvalue_fields.join(",")));

        // do we need to track scores?
        let track_scores =
            query.want_score() || query.limit().is_some() || query.min_score().is_some();

        if track_scores {
            url.push_str(&format!("&filter_path={}", SEARCH_FILTER_PATH));
        } else {
            url.push_str(&format!("&filter_path={}", SEARCH_FILTER_PATH_NO_SCORE));
        }

        // how should we sort the results?
        let mut sort_json = query.sort_json().cloned();

        // adjust the chunk size we want Elasticsearch to return for us
        // to be that of our limit
        match query.limit() {
            Some(limit) if limit == 0 => {
                // with a limit of zero, we can avoid going to Elasticsearch at all
                // and just return a (mostly) None'd response
                return Ok(ElasticsearchSearchResponse {
                    elasticsearch: None,
                    limit: Some(0),
                    offset: None,
                    track_scores,
                    should_sort_hits,
                    scroll_id: None,
                    shards: None,
                    hits: None,
                    fast_terms: None,
                });
            }
            Some(limit) if limit <= elasticsearch.options.max_result_window() as u64 => {
                url.push_str(&format!("&size={}", limit));
                // if we don't already have a sort_json, create one to
                // order by _score desc
                if sort_json.is_none() {
                    sort_json = Some(json!([{"_score": "desc"}]));
                }
            }
            _ => {
                url.push_str(&format!(
                    "&size={}",
                    elasticsearch.options.max_result_window()
                ));
            }
        }

        let have_user_sort = sort_json.is_some();

        // if we made it this far and never set a sort, we'll hard-code
        // sorting in the index _doc order so that we return rows in heap
        // order (assuming the index was created with index.sort.field=zdb_ctid),
        // which is much nicer to disk I/O
        if sort_json.is_none() {
            // determine if we also need to sort the hits as they're returned from ES
            //
            // We only need to do that if the table contains a column of type json or jsonb
            // as indexes with nested objects can't have an index-organized sort
            let indexrel = elasticsearch.heap_relation();
            for att in indexrel.tuple_desc() {
                if att.is_dropped() {
                    continue;
                }
                let typoid = att.type_oid();
                if typoid == PgBuiltInOids::JSONOID.oid() || typoid == PgBuiltInOids::JSONBOID.oid()
                {
                    // yes, we gotta sort the hits
                    should_sort_hits = true;
                    break;
                }
            }

            if should_sort_hits {
                // we need to sort the hits as they're returned from each scroll request, so
                // lets elide any sorting within Elasticsearch and just return the docs back
                // in index order
                sort_json = Some(json!([{"_doc": "asc"}]));
            } else {
                // we do NOT need to sort the hits because we know the index is already sorted
                // by zdb_ctid, so we apply a sort on that field to tell ES to return the docs
                // in that order.  This is essentially "free" for ES since the index is already
                // organized by zdb_ctid
                sort_json = Some(json!([{"zdb_ctid": "asc"}]));
            }
        }

        #[derive(Serialize)]
        struct Body {
            track_scores: bool,

            #[serde(skip_serializing_if = "Option::is_none")]
            min_score: Option<f64>,

            #[serde(skip_serializing_if = "Option::is_none")]
            sort: Option<Value>,

            query: Value,

            #[serde(skip_serializing_if = "Option::is_none")]
            highlight: Option<HashMap<&'static str, HashMap<String, Value>>>,
        }

        let limit = query.limit();
        let offset = query.offset();
        let min_score = query.min_score();

        let highlight = if query.has_highlights() {
            let mut map = HashMap::new();
            map.insert("fields", query.highlights().clone());
            Some(map)
        } else {
            None
        };

        // we only need to apply the visibility clause for searching if the query has a limit
        // in the future, maybe we can look at some table dead tuple stats and decide
        // to apply the clause if the table has a high percentage of them
        let query_dsl = if limit.is_some() {
            apply_visibility_clause(&elasticsearch, query, false)
        } else {
            query.take_query_dsl()
        };

        let can_do_fastterms = ZDB_ACCELERATOR.get()
            && limit.is_none()
            && offset.is_none()
            && highlight.is_none()
            && min_score.is_none()
            && have_extra_fields == false
            && track_scores == false
            && have_user_sort == false;

        let body = Body {
            track_scores,
            min_score,
            sort: sort_json,
            query: query_dsl,
            highlight,
        };

        ElasticsearchSearchRequest::get_hits(
            can_do_fastterms,
            url,
            limit,
            offset,
            elasticsearch,
            track_scores,
            should_sort_hits,
            json! { body },
        )
    }

    fn scroll(
        elasticsearch: &Elasticsearch,
        scroll_id: &str,
        track_scores: bool,
        should_sort_hits: bool,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        let mut url = String::new();
        url.push_str(elasticsearch.options.url());
        url.push_str("_search/scroll");
        url.push_str("?filter_path=");
        if track_scores {
            url.push_str(SEARCH_FILTER_PATH);
        } else {
            url.push_str(SEARCH_FILTER_PATH_NO_SCORE);
        }

        ElasticsearchSearchRequest::get_hits(
            false,
            url,
            None,
            None,
            elasticsearch,
            track_scores,
            should_sort_hits,
            json! {
                {
                    "scroll": "5s",
                    "scroll_id": scroll_id
                }
            },
        )
    }

    fn get_hits(
        fast_terms: bool,
        mut url: String,
        limit: Option<u64>,
        offset: Option<u64>,
        elasticsearch: &Elasticsearch,
        track_scores: bool,
        should_sort_hits: bool,
        body: serde_json::Value,
    ) -> std::result::Result<ElasticsearchSearchResponse, ElasticsearchError> {
        if fast_terms {
            let mut url = String::new();
            url.push_str(&elasticsearch.base_url());
            url.push_str("/_fastterms");

            Elasticsearch::execute_json_request(
                Elasticsearch::client().post(&url),
                Some(body),
                |mut body| {
                    use byteorder::*;

                    let many = body
                        .read_i32::<BigEndian>()
                        .expect("failed to read _fastterms header");

                    let mut fast_terms =
                        Vec::with_capacity(many.try_into().expect("unable to use 'many' as usize"));

                    if let Ok(mut ctid) = read_vlong(&mut body) {
                        fast_terms.push(ctid);
                        while let Ok(diff) = read_vlong(&mut body) {
                            ctid += diff;
                            fast_terms.push(ctid);
                        }
                    }

                    Ok(ElasticsearchSearchResponse {
                        elasticsearch: None,
                        limit: None,
                        offset: None,
                        track_scores: false,
                        should_sort_hits: false,
                        scroll_id: None,
                        shards: None,
                        hits: None,
                        fast_terms: Some(fast_terms),
                    })
                },
            )
        } else {
            url.push_str("&format=cbor");
            Elasticsearch::execute_json_request(
                Elasticsearch::client().post(&url),
                Some(body),
                |body| {
                    let mut response: ElasticsearchSearchResponse =
                        serde_cbor::from_reader(body).expect("failed to deserialize CBOR response");

                    // make sure there's no failures listed in the response
                    if response.shards.is_some()
                        && response.shards.as_ref().unwrap().failures.is_some()
                    {
                        // ES gave us an error so report it back to the user
                        let error_string = serde_json::to_string_pretty(
                            &response.shards.as_ref().unwrap().failures.as_ref().unwrap(),
                        )
                        .unwrap_or_else(|e| format!("{:?}", e));
                        return Err(ElasticsearchError(None, error_string));
                    }

                    // assign a clone of our ES client to the response too,
                    // for future use during iteration
                    response.elasticsearch = Some(elasticsearch.clone());
                    response.limit = limit;
                    response.offset = offset;
                    response.should_sort_hits = should_sort_hits;
                    response.track_scores = track_scores;

                    Ok(response)
                },
            )
        }
    }
}

impl ElasticsearchSearchResponse {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self.hits.as_ref() {
            Some(hits) => hits.total.value as usize,
            None => 0,
        }
    }
}

pub struct Scroller {
    receiver: std::sync::mpsc::Receiver<Vec<InnerHit>>,
    current_hits: Option<std::vec::IntoIter<InnerHit>>,
    terminate: Arc<AtomicBool>,
    should_sort_hits: bool,
}

impl Scroller {
    fn new(
        orig_elasticsearch: Elasticsearch,
        orig_scroll_id: Option<String>,
        mut initial_hits: Vec<InnerHit>,
        track_scores: bool,
        should_sort_hits: bool,
    ) -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        let terminate_arc = Arc::new(AtomicBool::new(false));

        // spawn a thread to continually get the next scroll chunk from Elasticsearch
        // until there's no more to get
        let mut scroll_id = orig_scroll_id.clone();
        let elasticsearch = orig_elasticsearch;
        let terminate = terminate_arc.clone();
        std::thread::spawn(move || {
            while let Some(sid) = scroll_id {
                if terminate.load(Ordering::SeqCst) {
                    break;
                }

                match ElasticsearchSearchRequest::scroll(
                    &elasticsearch,
                    &sid,
                    track_scores,
                    should_sort_hits,
                ) {
                    Ok(response) => {
                        scroll_id = response.scroll_id;

                        match response.hits.unwrap().hits {
                            Some(inner_hits) => {
                                // send the hits across the scroll_sender channel
                                // so they can be iterated by the main thread
                                if sender.send(inner_hits).is_err() {
                                    // failed to send the hits over 'sender'.
                                    // nothing else we can do here
                                    break;
                                }
                            }
                            None => {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }

                if scroll_id.is_none() {
                    break;
                }
            }

            // we're done scrolling, so drop the sender
            // which will cause the receiver to terminate as soon
            // as it's drained
            drop(sender);

            if let Some(scroll_id) = orig_scroll_id {
                Elasticsearch::execute_json_request(
                    Elasticsearch::client().delete(&format!(
                        "{}_search/scroll/{}",
                        elasticsearch.url(),
                        scroll_id
                    )),
                    None,
                    |_| Ok(()),
                )
                .expect("failed to delete scroll");
            }
        });

        Scroller {
            receiver,
            current_hits: Some({
                if should_sort_hits {
                    Scroller::sort_hits(&mut initial_hits);
                }
                initial_hits.into_iter()
            }),
            terminate: terminate_arc,
            should_sort_hits,
        }
    }

    #[inline]
    fn next(
        &mut self,
    ) -> Option<(
        f64,
        u64,
        Option<Fields>,
        Option<HashMap<String, Vec<String>>>,
    )> {
        while let Some(current_hits) = self.current_hits.as_mut() {
            match current_hits.next() {
                Some(next_hit) => {
                    match next_hit.into_tuple() {
                        Some(tuple) => {
                            // we have a valid tuple
                            return Some(tuple);
                        }
                        None => {
                            // this likely represents the "zdb_aborted_xids" hit, so we'll just
                            // loop around to the next hit
                            continue;
                        }
                    }
                }
                None => {
                    self.current_hits = match self.receiver.recv() {
                        // the receiver has more hits for us
                        Ok(mut hits) => Some({
                            if self.should_sort_hits {
                                Scroller::sort_hits(&mut hits);
                            }
                            hits.into_iter()
                        }),

                        // the receiver is probably closed now.  we don't care about the error
                        Err(_) => None,
                    }
                }
            }
        }

        None
    }

    fn sort_hits(vec: &mut Vec<InnerHit>) {
        use rayon::prelude::*;

        vec.par_sort_unstable_by(|a, b| {
            if let Some(a) = a.fields.as_ref() {
                if let Some(b) = b.fields.as_ref() {
                    let a = a.zdb_ctid.unwrap_or_default()[0];
                    let b = b.zdb_ctid.unwrap_or_default()[0];
                    return a.cmp(&b);
                }
            }
            core::cmp::Ordering::Equal
        })
    }
}

pub struct SearchResponseIntoIter {
    scroller: Option<Scroller>,
    limit: Option<u64>,
    cnt: u64,
    fast_terms: Option<std::vec::IntoIter<u64>>,
    // fast_terms: Option<std::vec::IntoIter<[u8; 6]>>,
    // fast_terms: Option<roaring::treemap::IntoIter>,
}

impl Drop for SearchResponseIntoIter {
    fn drop(&mut self) {
        if let Some(scroller) = &self.scroller {
            scroller.terminate.store(true, Ordering::SeqCst);
        }
    }
}

impl Iterator for SearchResponseIntoIter {
    type Item = (
        f64,
        u64,
        Option<Fields>,
        Option<HashMap<String, Vec<String>>>,
    );

    fn next(&mut self) -> Option<Self::Item> {
        match self.fast_terms.as_mut() {
            Some(fast_terms) => fast_terms.next().map_or(None, |ctid| {
                // use byteorder::*;
                // let slice = &mut ctid.as_ref();
                // let blockno = slice.read_u32::<LittleEndian>().unwrap() as u64;
                // let offno = slice.read_u16::<LittleEndian>().unwrap() as u64;
                // let ctid = (blockno << 32) | offno;
                Some((0.0, ctid, None, None))
            }),
            None => {
                if let Some(limit) = self.limit {
                    if self.cnt >= limit {
                        // we've reached our limit
                        return None;
                    }
                }

                let scroller = self.scroller.as_mut().unwrap();
                let item = scroller.next();
                self.cnt += 1;
                item
            }
        }
    }
}

impl IntoIterator for ElasticsearchSearchResponse {
    type Item = (
        f64,
        u64,
        Option<Fields>,
        Option<HashMap<String, Vec<String>>>,
    );
    type IntoIter = SearchResponseIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        if self.fast_terms.is_some() {
            SearchResponseIntoIter {
                scroller: None,
                limit: self.limit,
                cnt: 0,
                fast_terms: Some(self.fast_terms.unwrap().into_iter()),
            }
        } else if self.elasticsearch.is_none() {
            SearchResponseIntoIter {
                scroller: None,
                limit: Some(0),
                cnt: 0,
                fast_terms: None,
            }
        } else {
            let mut scroller = Scroller::new(
                self.elasticsearch.expect("no elasticsearch"),
                self.scroll_id,
                self.hits.unwrap().hits.unwrap_or_default(),
                self.track_scores,
                self.should_sort_hits,
            );

            // fast forward to our offset -- using the ?from= ES request parameter doesn't work with scroll requests
            if let Some(offset) = self.offset {
                for _ in 0..offset {
                    if scroller.next().is_none() {
                        break;
                    }
                }
            }

            SearchResponseIntoIter {
                scroller: Some(scroller),
                limit: self.limit,
                cnt: 0,
                fast_terms: None,
            }
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use pgx::*;

    #[pg_test]
    #[initialize(es = true)]
    fn test_limit_none() {
        Spi::run("CREATE TABLE test_limit AS SELECT * FROM generate_series(1, 10001);");
        Spi::run("CREATE INDEX idxtest_limit ON test_limit USING zombodb ((test_limit.*));");
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_limit WHERE test_limit ==> dsl.match_all(); ",
        )
        .expect("failed to get SPI result");
        assert_eq!(count, 10_001);
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_limit_exact() {
        Spi::run("CREATE TABLE test_limit AS SELECT * FROM generate_series(1, 10001);");
        Spi::run("CREATE INDEX idxtest_limit ON test_limit USING zombodb ((test_limit.*));");
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_limit WHERE test_limit ==> dsl.limit(10001, dsl.match_all()); ",
        )
        .expect("failed to get SPI result");
        assert_eq!(count, 10001);
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_limit_10() {
        Spi::run("CREATE TABLE test_limit AS SELECT * FROM generate_series(1, 10001);");
        Spi::run("CREATE INDEX idxtest_limit ON test_limit USING zombodb ((test_limit.*));");
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_limit WHERE test_limit ==> dsl.limit(10, dsl.match_all()); ",
        )
        .expect("failed to get SPI result");
        assert_eq!(count, 10);
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_limit_0() {
        Spi::run("CREATE TABLE test_limit AS SELECT * FROM generate_series(1, 10001);");
        Spi::run("CREATE INDEX idxtest_limit ON test_limit USING zombodb ((test_limit.*));");
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_limit WHERE test_limit ==> dsl.limit(0, dsl.match_all()); ",
        )
        .expect("failed to get SPI result");
        assert_eq!(count, 0);
    }

    #[pg_test(error = "limit must be positive")]
    #[initialize(es = true)]
    fn test_limit_negative() {
        Spi::run("CREATE TABLE test_limit AS SELECT * FROM generate_series(1, 10001);");
        Spi::run("CREATE INDEX idxtest_limit ON test_limit USING zombodb ((test_limit.*));");
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_limit WHERE test_limit ==> dsl.limit(-1, dsl.match_all()); ",
        )
        .expect("failed to get SPI result");
        panic!("executed a search with a negative limit.  count={}", count);
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_sort_desc() {
        Spi::run("CREATE TABLE test_sort AS SELECT * FROM generate_series(1, 100);");
        Spi::run("CREATE INDEX idxtest_sort ON test_sort USING zombodb ((test_sort.*));");
        Spi::connect(|client| {
            let mut table = client.select("SELECT * FROM test_sort WHERE test_sort ==> dsl.sort('generate_series', 'desc', dsl.match_all());", None, None).first();

            let mut previous = table.get_one::<i64>().unwrap();
            while table.next().is_some() {
                let current = table.get_datum::<i64>(1).expect("value was NULL");

                assert!(current < previous);
                previous = current;
            }
            Ok(Some(()))
        });
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_min_score_cutoff() {
        Spi::run("CREATE TABLE test_minscore AS SELECT * FROM generate_series(1, 100);");
        Spi::run(
            "CREATE INDEX idxtest_minscore ON test_minscore USING zombodb ((test_minscore.*));",
        );
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_minscore WHERE test_minscore ==> dsl.min_score(2, dsl.match_all()); ",
        )
            .expect("failed to get SPI result");
        assert_eq!(count, 0);
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_offset_scan() {
        Spi::run("CREATE TABLE test_offset AS SELECT * FROM generate_series(1, 100);");
        Spi::run("CREATE INDEX idxtest_offset ON test_offset USING zombodb ((test_offset.*));");
        let count = Spi::get_one::<i64>(
            "SELECT * FROM test_offset WHERE test_offset ==> dsl.sort('generate_series', 'asc', dsl.offset(10, dsl.match_all()));"
        )
            .expect("failed to get SPI result");
        assert_eq!(count, 11);
    }

    #[pg_test]
    #[initialize(es = true)]
    fn test_offset_overflow() {
        Spi::run("CREATE TABLE test_offset AS SELECT * FROM generate_series(1, 100);");
        Spi::run("CREATE INDEX idxtest_offset ON test_offset USING zombodb ((test_offset.*));");
        assert!(Spi::get_one::<i64>(
            "SELECT * FROM test_offset WHERE test_offset ==> dsl.sort('generate_series', 'asc', dsl.offset(1000, dsl.match_all()));"
        ).is_none());
    }
}
