use pgx::{
    pg_sys, register_xact_callback, spi, spi::SpiTupleTable, IntoDatum, PgOid, PgRelation,
    PgXactCallbackEvent, Spi,
};

use crate::elasticsearch::Elasticsearch;
use crate::gucs::ZDB_LOG_LEVEL;
use crate::utils::{is_non_shadow_zdb_index, lookup_zdb_extension_oid};

pub fn drop_index(index: &PgRelation) {
    // we can only delete the remote index for actual ZDB indices
    if is_non_shadow_zdb_index(index) {
        // when the transaction commits, we'll make a best effort to delete this index
        // from its remote Elasticsearch server
        let es = Elasticsearch::new(index);
        register_xact_callback(PgXactCallbackEvent::Commit, move || {
            ZDB_LOG_LEVEL.get().log(&format!(
                "[zombodb] Deleting remote index: {}",
                es.base_url()
            ));

            // we're just going to assume it worked, throwing away any error
            // because raising an elog(ERROR) here would cause Postgres to panic
            es.delete_index().execute().ok();
        });
    }
}

pub fn drop_table(table: &PgRelation) {
    for index in table.indices(pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE) {
        drop_index(&index);
    }
}

pub fn drop_schema(schema_oid: pg_sys::Oid) {
    Spi::connect(|client| {
        let table = client.select(
            "select oid from pg_class
                    where relnamespace = $1
                      and relam = (select oid from pg_am where amname = 'zombodb')",
            None,
            Some(vec![(PgOid::from(pg_sys::OIDOID), schema_oid.into_datum())]),
        )?;
        drop_index_oids(table);
        Ok::<_, spi::Error>(())
    })
    .expect("SPI failed")
}

pub fn drop_extension(extension_oid: pg_sys::Oid) {
    if extension_oid == lookup_zdb_extension_oid() {
        Spi::connect(|client| {
            let table = client.select(
                "select oid from pg_class
                    where relam = (select oid from pg_am where amname = 'zombodb')",
                None,
                None,
            )?;
            drop_index_oids(table);
            Ok::<_, spi::Error>(())
        })
        .expect("SPI failed");
    }
}

fn drop_index_oids(mut table: SpiTupleTable) {
    while table.next().is_some() {
        let oid = table
            .get_one::<pg_sys::Oid>()
            .expect("SPI failed")
            .expect("returned index oid is NULL");
        let index =
            unsafe { PgRelation::with_lock(oid, pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE) };
        drop_index(&index);
    }
}
