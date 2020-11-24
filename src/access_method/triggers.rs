use crate::executor_manager::get_executor_manager;
use pgx::pg_sys::AsPgCStr;
use pgx::*;
use std::ffi::CStr;

/// ```sql
/// CREATE OR REPLACE FUNCTION zdb.zdb_update_trigger() RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_update_trigger_wrapper';
/// ```
#[pg_extern]
fn zdb_update_trigger(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let trigdata: PgBox<pg_sys::TriggerData> = PgBox::from_pg(
        unsafe { fcinfo.as_ref() }.expect("fcinfo is NULL").context as *mut pg_sys::TriggerData,
    );
    let tg_trigger = PgBox::from_pg(trigdata.tg_trigger);

    /* make sure it's called as a trigger at all */
    if !called_as_trigger(fcinfo) {
        error!("zdb_update_trigger: not called by trigger manager");
    }

    if !trigger_fired_by_update(trigdata.tg_event) {
        error!("zdb_update_trigger: can only be fired for UPDATE triggers");
    }

    if !trigger_fired_before(trigdata.tg_event) {
        error!("zdb_update_trigger: can only be fired as a BEFORE trigger");
    }

    if tg_trigger.tgnargs != 1 {
        error!("zdb_update_trigger: called with incorrect number of arguments");
    }

    let args =
        unsafe { std::slice::from_raw_parts(tg_trigger.tgargs, tg_trigger.tgnargs as usize) };
    let index_relid_str = unsafe { CStr::from_ptr(args[0] as *const i8) }
        .to_str()
        .unwrap();
    let index_relid = str::parse::<pg_sys::Oid>(index_relid_str).expect("malformed oid");

    let bulk = get_executor_manager().checkout_bulk_context(index_relid);
    bulk.bulk
        .update(
            (unsafe { *trigdata.tg_trigtuple }).t_self,
            unsafe { pg_sys::GetCurrentCommandId(true) },
            xid_to_64bit(unsafe { pg_sys::GetCurrentTransactionId() }),
        )
        .expect("failed to queue index update command");

    trigdata.tg_newtuple as pg_sys::Datum
}

/// ```sql
/// CREATE OR REPLACE FUNCTION zdb.zdb_delete_trigger() RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_delete_trigger_wrapper';
/// ```
#[pg_extern]
fn zdb_delete_trigger(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let trigdata: PgBox<pg_sys::TriggerData> = PgBox::from_pg(
        unsafe { fcinfo.as_ref() }.expect("fcinfo is NULL").context as *mut pg_sys::TriggerData,
    );
    let tg_trigger = PgBox::from_pg(trigdata.tg_trigger);

    /* make sure it's called as a trigger at all */
    if !called_as_trigger(fcinfo) {
        error!("zdb_delete_trigger: not called by trigger manager");
    }

    if !trigger_fired_by_delete(trigdata.tg_event) {
        error!("zdb_delete_trigger: can only be fired for DELETE triggers");
    }

    if !trigger_fired_before(trigdata.tg_event) {
        error!("zdb_delete_trigger: can only be fired as a BEFORE trigger");
    }

    if tg_trigger.tgnargs != 1 {
        error!("zdb_delete_trigger: called with incorrect number of arguments");
    }

    let args =
        unsafe { std::slice::from_raw_parts(tg_trigger.tgargs, tg_trigger.tgnargs as usize) };
    let index_relid_str = unsafe { CStr::from_ptr(args[0] as *const i8) }
        .to_str()
        .unwrap();
    let index_relid = str::parse::<pg_sys::Oid>(index_relid_str).expect("malformed oid");

    let bulk = get_executor_manager().checkout_bulk_context(index_relid);
    bulk.bulk
        .update(
            (unsafe { *trigdata.tg_trigtuple }).t_self,
            unsafe { pg_sys::GetCurrentCommandId(true) },
            xid_to_64bit(unsafe { pg_sys::GetCurrentTransactionId() }),
        )
        .expect("failed to queue index delete command");

    trigdata.tg_trigtuple as pg_sys::Datum
}

pub fn create_triggers(index_relation: &PgRelation) {
    if !trigger_exists(index_relation, "zdb_update_trigger") {
        let trigger_oid = create_update_trigger(&index_relation);
        create_trigger_dependency(index_relation.oid(), trigger_oid);
    }

    if !trigger_exists(index_relation, "zdb_delete_trigger") {
        let trigger_oid = create_delete_trigger(&index_relation);
        create_trigger_dependency(index_relation.oid(), trigger_oid);
    }
}

fn trigger_exists(index_relation: &PgRelation, trigger_name: &str) -> bool {
    let heap_oid = index_relation.heap_relation().unwrap().oid();
    Spi::get_one::<bool>(&format!(
        "SELECT count(*) > 0 FROM pg_trigger WHERE tgrelid = {} AND tgname LIKE '{}%'",
        heap_oid, trigger_name
    ))
    .expect("failed to determine if trigger already exists")
}

fn create_update_trigger(index_relation: &PgRelation) -> pg_sys::Oid {
    create_trigger(
        index_relation,
        "zdb_update_trigger",
        "zdb_update_trigger",
        index_relation.oid(),
        pg_sys::TRIGGER_TYPE_UPDATE,
    )
}

fn create_delete_trigger(index_relation: &PgRelation) -> pg_sys::Oid {
    create_trigger(
        index_relation,
        "zdb_delete_trigger",
        "zdb_delete_trigger",
        index_relation.oid(),
        pg_sys::TRIGGER_TYPE_DELETE,
    )
}

fn create_trigger(
    index_relation: &PgRelation,
    trigger_name: &str,
    function_name: &str,
    trigger_arg: pg_sys::Oid,
    events: u32,
) -> pg_sys::Oid {
    let relrv = unsafe {
        pg_sys::makeRangeVar(
            index_relation.namespace().as_pg_cstr(),
            index_relation.name().as_pg_cstr(),
            -1,
        )
    };
    let mut args = PgList::new();
    let mut funcname = PgList::new();

    let trigger_arg_string = unsafe { pg_sys::makeString(trigger_arg.to_string().as_pg_cstr()) };

    args.push(trigger_arg_string);

    unsafe {
        funcname.push(pg_sys::makeString("zdb".as_pg_cstr()));
    }
    unsafe {
        funcname.push(pg_sys::makeString(function_name.as_pg_cstr()));
    }

    let mut tgstmt = PgBox::<pg_sys::CreateTrigStmt>::alloc_node(pg_sys::NodeTag_T_CreateTrigStmt);
    tgstmt.trigname = PgMemoryContexts::CurrentMemoryContext.pstrdup(trigger_name);
    tgstmt.relation = relrv;
    tgstmt.funcname = funcname.into_pg();
    tgstmt.args = args.into_pg();
    tgstmt.row = true;
    tgstmt.timing = pg_sys::TRIGGER_TYPE_BEFORE as i16;
    tgstmt.events = events as i16;

    #[cfg(feature = "pg10")]
    let object_address = unsafe {
        pg_sys::CreateTrigger(
            tgstmt.into_pg(),
            std::ptr::null_mut(),
            index_relation.heap_relation().unwrap().oid(),
            pg_sys::InvalidOid,
            pg_sys::InvalidOid,
            pg_sys::InvalidOid,
            true,
        )
    };

    #[cfg(not(feature = "pg10"))]
    let object_address = unsafe {
        pg_sys::CreateTrigger(
            tgstmt.into_pg(),
            std::ptr::null_mut(),
            index_relation.heap_relation().unwrap().oid(),
            pg_sys::InvalidOid,
            pg_sys::InvalidOid,
            pg_sys::InvalidOid,
            pg_sys::InvalidOid,
            pg_sys::InvalidOid,
            std::ptr::null_mut(),
            true,
            false,
        )
    };

    // Make the new trigger visible within this session
    unsafe {
        pg_sys::CommandCounterIncrement();
    }

    object_address.objectId
}

fn create_trigger_dependency(index_rel_oid: pg_sys::Oid, trigger_oid: pg_sys::Oid) {
    let index_address = pg_sys::ObjectAddress {
        classId: pg_sys::RelationRelationId,
        objectId: index_rel_oid,
        objectSubId: 0,
    };
    let trigger_address = pg_sys::ObjectAddress {
        classId: pg_sys::TriggerRelationId,
        objectId: trigger_oid,
        objectSubId: 0,
    };

    unsafe {
        pg_sys::recordDependencyOn(
            &trigger_address,
            &index_address,
            pg_sys::DependencyType_DEPENDENCY_INTERNAL,
        )
    }
}
