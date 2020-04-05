use crate::executor_manager::get_executor_manager;
use crate::scoring::WantScoresWalker;
use pgx::{pg_sys, register_hook, HookResult, PgBox, PgHooks};

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
        get_executor_manager().pop_query();
        prev_hook(query_desc)
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
