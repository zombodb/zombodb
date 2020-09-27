use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use crate::gucs::ZDB_LOG_LEVEL;
use crate::query_dsl::bool::dsl::{and, noteq};
use crate::query_dsl::range::dsl::range_numeric;
use crate::query_dsl::terms_lookup::dsl::terms_lookup;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    let info = PgBox::from_pg(info);
    let index_relation = unsafe { PgRelation::from_pg(info.index) };
    let elasticsearch = Elasticsearch::new(&index_relation);
    let options = ZDBIndexOptions::from(&index_relation);
    let es_index_name = options.index_name();
    let oldest_xmin = unsafe {
        pg_sys::TransactionIdLimitedForOldSnapshots(
            pg_sys::GetOldestXmin(info.index, pg_sys::PROCARRAY_FLAGS_VACUUM as i32),
            info.index,
        )
    };

    // first things first, VACUUM requires that the index be fully refreshed
    elasticsearch
        .refresh_index()
        .execute()
        .expect("failed to refresh index");

    let mut bulk = elasticsearch.start_bulk();

    // Find all rows with what we think is an *aborted* xmin
    //
    // These rows can be deleted
    let by_xmin = delete_by_xmin(
        &index_relation,
        &elasticsearch,
        &es_index_name,
        oldest_xmin,
        &mut bulk,
    );

    // Find all rows with what we think is a *committed* xmax
    //
    // These rows can be deleted
    let by_xmax = delete_by_xmax(
        &index_relation,
        &elasticsearch,
        &es_index_name,
        oldest_xmin,
        &mut bulk,
    );

    // Find all rows with what we think is an *aborted* xmax
    //
    // These rows can have their xmax reset to null because they're still live
    let vacuumed = vacuum_xmax(
        &index_relation,
        &elasticsearch,
        &es_index_name,
        oldest_xmin,
        &mut bulk,
    );

    // Finally, any "zdb_aborted_xid" value we have can be removed if it's
    // known to be aborted and no longer referenced anywhere in the index
    let aborted = remove_aborted_xids(&index_relation, &elasticsearch, oldest_xmin, &mut bulk);

    bulk.finish().expect("failed to finish vacuum");

    ZDB_LOG_LEVEL.get().log(&format!(
        "[zombodb] vacuum:  index={}, by_xmin={}, by_xmax={}, vacuumed={}, aborted_xids_removed={}",
        elasticsearch.base_url(),
        by_xmin,
        by_xmax,
        vacuumed,
        aborted
    ));
    result.into_pg()
}

#[pg_guard]
pub extern "C" fn amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();

    if stats.is_null() {
        ambulkdelete(info, result.as_ptr(), None, std::ptr::null_mut());
    }

    let info = PgBox::from_pg(info);
    let index_relation = unsafe { PgRelation::from_pg(info.index) };
    let elasticsearch = Elasticsearch::new(&index_relation);

    elasticsearch
        .expunge_deletes()
        .execute()
        .expect("failed to expunge deleted docs");

    result.into_pg()
}

fn remove_aborted_xids(
    index: &PgRelation,
    elasticsearch: &Elasticsearch,
    oldest_xmin: u32,
    bulk: &mut ElasticsearchBulkRequest,
) -> usize {
    #[derive(Deserialize, Debug)]
    struct Source {
        zdb_aborted_xids: Vec<u64>,
    }
    #[derive(Deserialize, Debug)]
    struct ZdbAbortedXids {
        #[serde(rename = "_source")]
        source: Source,
    }
    let mut cnt = 0;
    let aborted_xids_doc = elasticsearch
        .get_document::<ZdbAbortedXids>("zdb_aborted_xids", false)
        .execute()
        .expect("failed to get the zdb_aborted_xids doc");

    if let Some(aborted_xids_doc) = aborted_xids_doc {
        let mut xids_to_remove = Vec::new();
        for xid64 in aborted_xids_doc.source.zdb_aborted_xids.into_iter() {
            let xid = xid64 as pg_sys::TransactionId;
            if unsafe { pg_sys::TransactionIdPrecedes(xid, oldest_xmin) }
                && unsafe { pg_sys::TransactionIdDidAbort(xid) }
                && !unsafe { pg_sys::TransactionIdDidCommit(xid) }
                && !unsafe { pg_sys::TransactionIdIsInProgress(xid) }
            {
                let xmin_cnt = elasticsearch
                    .count(
                        ZDBQuery::new_with_query_dsl(serde_json::json! {
                            {
                                "term": {
                                    "zdb_xmin": xid64
                                }
                            }
                        })
                        .prepare(index),
                    )
                    .execute()
                    .expect("failed to count xmin values");
                let xmax_cnt = elasticsearch
                    .count(
                        ZDBQuery::new_with_query_dsl(serde_json::json! {
                            {
                                "term": {
                                    "zdb_xmax": xid64
                                }
                            }
                        })
                        .prepare(index),
                    )
                    .execute()
                    .expect("failed to count xmax values");

                if xmin_cnt == 0 && xmax_cnt == 0 {
                    // all counts are zero, so they're gone!
                    xids_to_remove.push(xid64);
                }
            }
        }

        cnt = xids_to_remove.len();
        bulk.remove_aborted_xids(xids_to_remove)
            .expect("failed to remove aborted xids");
    }

    cnt
}

fn vacuum_xmax(
    index: &PgRelation,
    elasticsearch: &Elasticsearch,
    es_index_name: &str,
    oldest_xmin: u32,
    bulk: &mut ElasticsearchBulkRequest,
) -> usize {
    let mut cnt = 0;
    let vacuum_xmax_docs = elasticsearch
        .open_search(
            vac_by_aborted_xmax(&es_index_name, xid_to_64bit(oldest_xmin) as i64).prepare(index),
        )
        .execute_with_fields(vec!["zdb_xmax"])
        .expect("failed to search by xmax");
    for (_, ctid, fields, _) in vacuum_xmax_docs.into_iter() {
        check_for_interrupts!();

        if let Some(xmax) = fields.zdb_xmax {
            let xmax64 = xmax[0];
            let xmax = xmax64 as pg_sys::TransactionId;

            if unsafe { pg_sys::TransactionIdPrecedes(xmax, oldest_xmin) }
                && unsafe { pg_sys::TransactionIdDidAbort(xmax) }
                && !unsafe { pg_sys::TransactionIdDidCommit(xmax) }
                && !unsafe { pg_sys::TransactionIdIsInProgress(xmax) }
            {
                bulk.vacuum_xmax(ctid, xmax64)
                    .expect("failed to submit vacuum_xmax command");
                cnt += 1;
            }
        }
    }

    cnt
}

fn delete_by_xmax(
    index: &PgRelation,
    elasticsearch: &Elasticsearch,
    es_index_name: &str,
    oldest_xmin: u32,
    bulk: &mut ElasticsearchBulkRequest,
) -> usize {
    let mut cnt = 0;
    let delete_by_xmax_docs = elasticsearch
        .open_search(vac_by_xmax(&es_index_name, xid_to_64bit(oldest_xmin) as i64).prepare(index))
        .execute_with_fields(vec!["zdb_xmax"])
        .expect("failed to search by xmax");
    for (_, ctid, fields, _) in delete_by_xmax_docs.into_iter() {
        check_for_interrupts!();

        if let Some(xmax) = fields.zdb_xmax {
            let xmax64 = xmax[0];
            let xmax = xmax64 as pg_sys::TransactionId;

            if unsafe { pg_sys::TransactionIdPrecedes(xmax, oldest_xmin) }
                && unsafe { pg_sys::TransactionIdDidCommit(xmax) }
                && !unsafe { pg_sys::TransactionIdDidAbort(xmax) }
                && !unsafe { pg_sys::TransactionIdIsInProgress(xmax) }
            {
                bulk.delete_by_xmax(ctid, xmax64)
                    .expect("failed to submit delete_by_xmax command");
                cnt += 1;
            }
        }
    }

    cnt
}

fn delete_by_xmin(
    index: &PgRelation,
    elasticsearch: &Elasticsearch,
    es_index_name: &str,
    oldest_xmin: u32,
    bulk: &mut ElasticsearchBulkRequest,
) -> usize {
    let mut cnt = 0;
    let delete_by_xmin_docs = elasticsearch
        .open_search(vac_by_xmin(&es_index_name, xid_to_64bit(oldest_xmin) as i64).prepare(index))
        .execute_with_fields(vec!["zdb_xmin"])
        .expect("failed to search by xmin");
    for (_, ctid, fields, _) in delete_by_xmin_docs.into_iter() {
        check_for_interrupts!();

        if let Some(xmin) = fields.zdb_xmin {
            let xmin64 = xmin[0];
            let xmin = xmin64 as pg_sys::TransactionId;

            if unsafe { pg_sys::TransactionIdPrecedes(xmin, oldest_xmin) }
                && unsafe { pg_sys::TransactionIdDidAbort(xmin) }
                && !unsafe { pg_sys::TransactionIdDidCommit(xmin) }
                && !unsafe { pg_sys::TransactionIdIsInProgress(xmin) }
            {
                bulk.delete_by_xmin(ctid, xmin64)
                    .expect("failed to submit delete_by_xmin command");
                cnt += 1;
            }
        }
    }

    cnt
}

/// docs with aborted xmins
///
/// SELECT dsl.and(
///     dsl.range(field=>'zdb_xmin', lt=>xmin),
///     dsl.terms_lookup('zdb_xmin', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')
/// );
fn vac_by_xmin(es_index_name: &str, xmin: i64) -> ZDBQuery {
    and(vec![
        Some(range_numeric(
            "zdb_xmin",
            Some(xmin),
            None,
            None,
            None,
            None,
        )),
        Some(terms_lookup(
            "zdb_xmin",
            es_index_name,
            "zdb_aborted_xids",
            "zdb_aborted_xids",
            None,
        )),
    ])
}

/// docs with committed xmax
///
/// SELECT dsl.and(
///     dsl.range(field=>'zdb_xmax', lt=>xmax),
///     dsl.noteq(dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids'))
/// );
fn vac_by_xmax(es_index_name: &str, xmax: i64) -> ZDBQuery {
    and(vec![
        Some(range_numeric(
            "zdb_xmax",
            Some(xmax),
            None,
            None,
            None,
            None,
        )),
        Some(noteq(terms_lookup(
            "zdb_xmax",
            es_index_name,
            "zdb_aborted_xids",
            "zdb_aborted_xids",
            None,
        ))),
    ])
}

/// docs with aborted xmax
///
/// SELECT dsl.and(
///    dsl.range(field=>'zdb_xmax', lt=>xmax),
///    dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')
/// );
fn vac_by_aborted_xmax(es_index_name: &str, xmax: i64) -> ZDBQuery {
    and(vec![
        Some(range_numeric(
            "zdb_xmax",
            Some(xmax),
            None,
            None,
            None,
            None,
        )),
        Some(terms_lookup(
            "zdb_xmax",
            es_index_name,
            "zdb_aborted_xids",
            "zdb_aborted_xids",
            None,
        )),
    ])
}
