use crate::access_method::rewriter::rewrite_opexrs;
use crate::executor_manager::alter::{
    alter_indices, get_index_options_for_relation, get_index_options_for_schema,
};
use crate::executor_manager::drop::{drop_extension, drop_index, drop_schema, drop_table};
use crate::executor_manager::get_executor_manager;
use crate::walker::PlanWalker;
use pgx::*;

struct ZDBHooks;
impl PgHooks for ZDBHooks {
    fn executor_start(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        eflags: i32,
        prev_hook: fn(PgBox<pg_sys::QueryDesc>, i32) -> HookResult<()>,
    ) -> HookResult<()> {
        get_executor_manager().push_query(&query_desc);
        prev_hook(query_desc, eflags)
    }

    fn executor_end(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        prev_hook: fn(PgBox<pg_sys::QueryDesc>) -> HookResult<()>,
    ) -> HookResult<()> {
        let result = prev_hook(query_desc);
        get_executor_manager().pop_query();
        result
    }

    fn process_utility_hook(
        &mut self,
        pstmt: PgBox<pg_sys::PlannedStmt>,
        query_string: &std::ffi::CStr,
        context: u32,
        params: PgBox<pg_sys::ParamListInfoData>,
        query_env: PgBox<pg_sys::QueryEnvironment>,
        dest: PgBox<pg_sys::DestReceiver>,
        completion_tag: *mut i8,
        prev_hook: fn(
            PgBox<pg_sys::PlannedStmt>,
            &std::ffi::CStr,
            u32,
            PgBox<pg_sys::ParamListInfoData>,
            PgBox<pg_sys::QueryEnvironment>,
            PgBox<pg_sys::DestReceiver>,
            *mut i8,
        ) -> HookResult<()>,
    ) -> HookResult<()> {
        let utility_statement = PgBox::from_pg(pstmt.utilityStmt);

        let is_alter = is_a(utility_statement.as_ptr(), pg_sys::NodeTag_T_AlterTableStmt);
        let is_rename = is_a(utility_statement.as_ptr(), pg_sys::NodeTag_T_RenameStmt);
        let is_drop = is_a(utility_statement.as_ptr(), pg_sys::NodeTag_T_DropStmt);

        if is_alter {
            let alter = PgBox::from_pg(utility_statement.as_ptr() as *mut pg_sys::AlterTableStmt);
            let relid = unsafe {
                pg_sys::AlterTableLookupRelation(
                    alter.as_ptr(),
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                )
            };
            let rel = unsafe { PgRelation::open(relid) };
            let prev_options = get_index_options_for_relation(&rel);
            drop(rel);

            // call the prev hook to go ahead and apply the ALTER statement to the index
            let result = prev_hook(
                pstmt,
                query_string,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            );

            alter_indices(Some(prev_options));
            return result;
        } else if is_rename {
            let rename = PgBox::from_pg(utility_statement.as_ptr() as *mut pg_sys::RenameStmt);

            let prev_options = match rename.renameType {
                pg_sys::ObjectType_OBJECT_SCHEMA => {
                    let name = unsafe { std::ffi::CStr::from_ptr(rename.subname) };
                    Some(get_index_options_for_schema(
                        name.to_str().expect("invalid schema name"),
                    ))
                }
                pg_sys::ObjectType_OBJECT_TABLE
                | pg_sys::ObjectType_OBJECT_MATVIEW
                | pg_sys::ObjectType_OBJECT_INDEX => unsafe {
                    let relid = pg_sys::RangeVarGetRelidExtended(
                        rename.relation,
                        pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                        pg_sys::RVROption_RVR_MISSING_OK,
                        None,
                        std::ptr::null_mut(),
                    );

                    if relid != pg_sys::InvalidOid {
                        let rel = PgRelation::open(relid);
                        Some(get_index_options_for_relation(&rel))
                    } else {
                        None
                    }
                },
                _ => None,
            };

            // call the prev hook to go ahead and apply the ALTER statement to the index
            let result = prev_hook(
                pstmt,
                query_string,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            );

            alter_indices(prev_options);
            return result;
        } else if is_drop {
            let drop = PgBox::from_pg(utility_statement.as_ptr() as *mut pg_sys::DropStmt);

            match drop.removeType {
                pg_sys::ObjectType_OBJECT_TABLE
                | pg_sys::ObjectType_OBJECT_MATVIEW
                | pg_sys::ObjectType_OBJECT_INDEX
                | pg_sys::ObjectType_OBJECT_SCHEMA
                | pg_sys::ObjectType_OBJECT_EXTENSION => {
                    let objects = PgList::<pg_sys::Node>::from_pg(drop.objects);
                    for object in objects.iter_ptr() {
                        let mut rel = std::ptr::null_mut();
                        let address = unsafe {
                            pg_sys::get_object_address(
                                drop.removeType,
                                object,
                                &mut rel,
                                pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
                                drop.missing_ok,
                            )
                        };

                        if address.objectId == pg_sys::InvalidOid {
                            // this object no longer exists
                            continue;
                        }

                        match drop.removeType {
                            pg_sys::ObjectType_OBJECT_TABLE | pg_sys::ObjectType_OBJECT_MATVIEW => {
                                let rel = PgRelation::from_pg_owned(rel);
                                drop_table(&rel);
                            }
                            pg_sys::ObjectType_OBJECT_INDEX => {
                                let rel = PgRelation::from_pg_owned(rel);
                                drop_index(&rel)
                            }
                            pg_sys::ObjectType_OBJECT_SCHEMA => drop_schema(address.objectId),
                            pg_sys::ObjectType_OBJECT_EXTENSION => drop_extension(address.objectId),
                            _ => {}
                        }
                    }
                }

                _ => {}
            }
        }

        prev_hook(
            pstmt,
            query_string,
            context,
            params,
            query_env,
            dest,
            completion_tag,
        )
    }

    fn planner(
        &mut self,
        parse: PgBox<pg_sys::Query>,
        cursor_options: i32,
        bound_params: PgBox<pg_sys::ParamListInfoData>,
        prev_hook: fn(
            PgBox<pg_sys::Query>,
            i32,
            PgBox<pg_sys::ParamListInfoData>,
        ) -> HookResult<*mut pg_sys::PlannedStmt>,
    ) -> HookResult<*mut pg_sys::PlannedStmt> {
        PlanWalker::new().perform(&parse);
        let result = prev_hook(parse, cursor_options, bound_params);

        unsafe {
            rewrite_opexrs(result.inner.as_mut().unwrap());
        }

        result
    }
}

static mut HOOKS: ZDBHooks = ZDBHooks;

pub unsafe fn init_hooks() {
    register_hook(&mut HOOKS)
}
