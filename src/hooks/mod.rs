use crate::executor_manager::ExecutorManager;
use pgx::pg_sys::*;
use pgx::pg_try;
use pgx::*;

static mut HOOK: ZDBHooks = ZDBHooks { manager: None };
pub struct ZDBHooks {
    manager: Option<&'static mut ExecutorManager>,
}

pub unsafe fn register_hook(manager: &'static mut ExecutorManager) {
    HOOK.manager.replace(manager);
    pgx::register_hook(&mut HOOK);
}

impl ZDBHooks {
    fn manager(&mut self) -> &mut ExecutorManager {
        self.manager.as_mut().unwrap()
    }
}

impl PgHooks for ZDBHooks {
    fn executor_start(
        &mut self,
        query_desc: PgBox<QueryDesc>,
        eflags: i32,
        prev_hook: fn(PgBox<QueryDesc>, i32) -> HookResult<()>,
    ) -> HookResult<()> {
        info!("start: depth={}", self.manager().depth());
        self.manager().push();
        pg_try(|| prev_hook(query_desc, eflags)).unwrap_or_rethrow(|| self.manager().pop())
    }

    fn executor_run(
        &mut self,
        query_desc: PgBox<QueryDesc>,
        direction: i32,
        count: u64,
        execute_once: bool,
        prev_hook: fn(PgBox<QueryDesc>, i32, u64, bool) -> HookResult<()>,
    ) -> HookResult<()> {
        info!("run");
        self.manager().push();
        pg_try(|| prev_hook(query_desc, direction, count, execute_once))
            .unwrap_or_rethrow(|| self.manager().pop())
    }

    fn executor_finish(
        &mut self,
        query_desc: PgBox<QueryDesc>,
        prev_hook: fn(PgBox<QueryDesc>) -> HookResult<()>,
    ) -> HookResult<()> {
        info!("finish");
        pg_try(|| prev_hook(query_desc)).finally_or_rethrow(|| self.manager().pop())
    }

    fn executor_end(
        &mut self,
        query_desc: PgBox<QueryDesc>,
        prev_hook: fn(PgBox<QueryDesc>) -> HookResult<()>,
    ) -> HookResult<()> {
        info!("end");
        pg_try(|| prev_hook(query_desc)).finally_or_rethrow(|| self.manager().pop())
    }

    fn planner(
        &mut self,
        parse: PgBox<Query>,
        cursor_options: i32,
        bound_params: PgBox<ParamListInfoData>,
        prev_hook: fn(PgBox<Query>, i32, PgBox<ParamListInfoData>) -> HookResult<*mut PlannedStmt>,
    ) -> HookResult<*mut PlannedStmt> {
        info!("planner");
        prev_hook(parse, cursor_options, bound_params)
    }

    fn abort(&mut self) {
        self.manager().abort()
    }
}
