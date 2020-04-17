use crate::executor_manager::drop::{drop_extension, drop_index, drop_schema, drop_table};
use crate::executor_manager::get_executor_manager;
use crate::scoring::WantScoresWalker;
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
        if is_a(utility_statement.as_ptr(), pg_sys::NodeTag_T_DropStmt) {
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
        WantScoresWalker::new().perform(&parse);

        prev_hook(parse, cursor_options, bound_params)
    }
}

static mut HOOKS: ZDBHooks = ZDBHooks;

pub unsafe fn init_hooks() {
    register_hook(&mut HOOKS)
}
