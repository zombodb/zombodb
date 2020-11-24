use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::Elasticsearch;
use crate::mapping::{categorize_tupdesc, generate_default_mapping};
use crate::utils::{is_zdb_index, lookup_zdb_index_tupdesc};
use pgx::{
    pg_sys, register_xact_callback, warning, IntoDatum, PgBuiltInOids, PgRelation,
    PgXactCallbackEvent, Spi,
};

pub fn get_index_options_for_relation(relation: &PgRelation) -> Vec<ZDBIndexOptions> {
    let mut options = Vec::new();

    if relation.is_table() || relation.is_matview() {
        for index in relation.indicies(pg_sys::AccessShareLock as pg_sys::LOCKMODE) {
            if is_zdb_index(&index) {
                options.push(ZDBIndexOptions::from_relation(&index));
            }
        }
    } else if relation.is_index() && is_zdb_index(relation) {
        options.push(ZDBIndexOptions::from_relation(relation))
    }

    options
}

pub fn get_index_options_for_schema(name: &str) -> Vec<ZDBIndexOptions> {
    let mut options = Vec::new();

    Spi::connect(|client| {
        let mut table = client.select("select oid from pg_class
                    where relnamespace = (select oid from pg_namespace where nspname = $1::text::name)
                      and relam = (select oid from pg_am where amname = 'zombodb')", None, Some(vec![(PgBuiltInOids::TEXTOID.oid(), name.into_datum())]));
        while table.next().is_some() {
            let oid = table.get_one::<pg_sys::Oid>().expect("index oid is NULL");
            let index = PgRelation::with_lock(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            options.push(ZDBIndexOptions::from_relation(&index));
        }
        Ok(Some(()))
    });

    options
}

pub fn alter_indices(prev_options: Option<Vec<ZDBIndexOptions>>) {
    if let Some(prev_options) = prev_options {
        for (old_options, new_options, index) in prev_options.into_iter().map(|option| {
            let index =
                PgRelation::with_lock(option.oid(), pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            (option, ZDBIndexOptions::from_relation(&index), index)
        }) {
            if old_options.url() != new_options.url()
                && !unsafe { pg_sys::session_auth_is_superuser }
            {
                panic!("You must be a superuser to change the 'url' parameter")
            }

            if old_options.type_name() != new_options.type_name() {
                panic!("The 'type_name' index properly can only be set during CREATE INDEX")
            }

            if old_options.uuid() != new_options.uuid() {
                panic!("The 'uuid' index property cannot be changed")
            }

            if old_options.shards() != new_options.shards() {
                warning!("Number of shards changed from {} to {}.  You must issue a REINDEX before this change will take effect", old_options.shards(), new_options.shards());
            }

            let es = Elasticsearch::new(&index);

            // change the index settings
            es.update_settings()
                .execute()
                .expect("failed to update index settings");

            // modify the index mapping
            // this is not an operation we can un-do
            let tupdesc = lookup_zdb_index_tupdesc(&index);
            let heap_relation = index.heap_relation().expect("no heap relation for index!");
            let mut mapping = generate_default_mapping(&heap_relation);
            let _ = categorize_tupdesc(&tupdesc, &heap_relation, Some(&mut mapping));
            es.put_mapping(
                serde_json::to_value(&mapping).expect("failed to serialize mapping to json"),
            )
            .execute()
            .expect("failed to update index mapping");

            // if the user changed the alias
            if old_options.alias() != new_options.alias() {
                // add the index to its new alias
                es.add_alias(new_options.alias())
                    .execute()
                    .expect("failed to add index to new alias");

                // register a pre-commit callback which will remove the index from its old alias
                register_xact_callback(PgXactCallbackEvent::PreCommit, move || {
                    let alias = old_options.alias().to_owned();
                    let es = Elasticsearch::from_options(old_options);
                    es.remove_alias(&alias)
                        .execute()
                        .expect("failed to remove index from old alias");
                });

                // register an abort callback which will remove the index from the new alias
                // and restore its settings.  There's nothing we can do about the mapping change, unfortunately
                register_xact_callback(PgXactCallbackEvent::Abort, move || {
                    let alias = new_options.alias().to_owned();
                    let es = Elasticsearch::from_options(new_options);
                    es.update_settings()
                        .execute()
                        .expect("failed to restore index settings");

                    es.remove_alias(&alias)
                        .execute()
                        .expect("failed to remove index from new alias");
                });
            }
        }
    }
}
