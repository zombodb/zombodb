use crate::executor_manager::get_executor_manager;
use crate::utils::lookup_function;
use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable, parallel_safe)]
fn score(ctid: Option<pg_sys::ItemPointerData>, fcinfo: pg_sys::FunctionCallInfo) -> f64 {
    match ctid {
        Some(ctid) => {
            let score = match get_executor_manager().peek_query_state() {
                Some((query_desc, query_state)) => {
                    match query_state.lookup_heap_oid_for_first_field(*query_desc, fcinfo) {
                        Some(heap_oid) => Some(query_state.get_score(heap_oid, ctid)),
                        None => None,
                    }
                }
                None => None,
            };

            match score {
                Some(score) => score,
                None => {
                    panic!("zdb_score()'s argument is not a direct table ctid column reference")
                }
            }
        }
        None => 0.0f64,
    }
}

#[pg_extern(immutable, parallel_safe)]
fn want_scores(query: ZDBQuery) -> ZDBQuery {
    query.set_want_score(true)
}

pub struct WantScoresWalker {
    zdbquery_oid: pg_sys::Oid,
    zdb_score_oid: pg_sys::Oid,
    zdb_anyelement_cmp_func_oid: pg_sys::Oid,
    zdb_want_score_oid: pg_sys::Oid,
    in_te: usize,
    in_sort: usize,
    want_scores: bool,
}

impl WantScoresWalker {
    pub fn new() -> Self {
        let zdbquery_oid = unsafe {
            pg_sys::TypenameGetTypid(std::ffi::CString::new("zdbquery").unwrap().as_ptr())
        };

        WantScoresWalker {
            zdbquery_oid,
            zdb_score_oid: lookup_function(vec!["zdb", "score"], Some(vec![pg_sys::TIDOID]))
                .unwrap_or(pg_sys::InvalidOid),
            zdb_anyelement_cmp_func_oid: lookup_function(
                vec!["zdb", "anyelement_cmpfunc"],
                Some(vec![pg_sys::ANYELEMENTOID, zdbquery_oid]),
            )
            .unwrap_or(pg_sys::InvalidOid),
            zdb_want_score_oid: lookup_function(
                vec!["zdb", "want_scores"],
                Some(vec![zdbquery_oid]),
            )
            .unwrap_or(pg_sys::InvalidOid),
            in_te: 0,
            in_sort: 0,
            want_scores: false,
        }
    }

    pub fn perform(&mut self, query: &PgBox<pg_sys::Query>) {
        if self.zdb_score_oid == pg_sys::InvalidOid
            || self.zdb_anyelement_cmp_func_oid == pg_sys::InvalidOid
        {
            // nothing to do b/c one or both of our functions couldn't be found
            return;
        }

        if self.detect(query) {
            self.rewrite(query);
        }
    }

    fn detect(&mut self, query: &PgBox<pg_sys::Query>) -> bool {
        unsafe {
            pg_sys::query_tree_walker(
                query.as_ptr(),
                Some(want_scores_walker),
                self as *mut WantScoresWalker as void_mut_ptr,
                pg_sys::QTW_EXAMINE_RTES as i32,
            );
        }

        self.want_scores
    }

    fn rewrite(&mut self, query: &PgBox<pg_sys::Query>) {
        unsafe {
            pg_sys::query_tree_walker(
                query.as_ptr(),
                Some(rewrite_walker),
                self as *mut WantScoresWalker as void_mut_ptr,
                pg_sys::QTW_EXAMINE_RTES as i32,
            );
        }
    }
}

#[pg_guard]
unsafe extern "C" fn want_scores_walker(
    node: *mut pg_sys::Node,
    context_ptr: void_mut_ptr,
) -> bool {
    if node.is_null() {
        return false;
    }

    let mut context = PgBox::<WantScoresWalker>::from_pg(context_ptr as *mut WantScoresWalker);

    if is_a(node, pg_sys::NodeTag_T_TargetEntry) {
        context.in_te += 1;
        let rc = pg_sys::expression_tree_walker(node, Some(want_scores_walker), context_ptr);
        context.in_te -= 1;
        return rc;
    } else if is_a(node, pg_sys::NodeTag_T_SortBy) {
        context.in_sort += 1;
        let rc = pg_sys::expression_tree_walker(node, Some(want_scores_walker), context_ptr);
        context.in_sort -= 1;
        return rc;
    } else if is_a(node, pg_sys::NodeTag_T_FuncExpr) {
        let func_expr = PgBox::from_pg(node as *mut pg_sys::FuncExpr);

        if func_expr.funcid == context.zdb_score_oid {
            if context.in_te > 0 || context.in_sort > 0 {
                // TODO:  can we figure out which table's ctid column is the argument?
                context.want_scores = true;
            } else {
                panic!("zdb.score() can only be used as a target entry or as a sort");
            }
        }
    } else if is_a(node, pg_sys::NodeTag_T_RangeTblEntry) {
        // allow range_table_walker to continue
        return false;
    } else if is_a(node, pg_sys::NodeTag_T_Query) {
        return pg_sys::query_tree_walker(
            node as *mut pg_sys::Query,
            Some(want_scores_walker),
            context_ptr,
            pg_sys::QTW_EXAMINE_RTES as i32,
        );
    }

    return pg_sys::expression_tree_walker(node, Some(want_scores_walker), context_ptr);
}

#[pg_guard]
unsafe extern "C" fn rewrite_walker(node: *mut pg_sys::Node, context_ptr: void_mut_ptr) -> bool {
    if node.is_null() {
        return false;
    }

    let context = PgBox::<WantScoresWalker>::from_pg(context_ptr as *mut WantScoresWalker);

    if is_a(node, pg_sys::NodeTag_T_OpExpr) {
        let opexpr = PgBox::from_pg(node as *mut pg_sys::OpExpr);

        if opexpr.opfuncid == context.zdb_anyelement_cmp_func_oid {
            // it's our ==> operator
            let mut args_list = PgList::<pg_sys::Node>::from_pg(opexpr.args);
            let first_arg = args_list.get_ptr(0);

            match first_arg {
                Some(first_arg) => {
                    if is_a(first_arg, pg_sys::NodeTag_T_Var) {
                        // wrap the right-hand-side of the ==> operator in zdb.want_score(...)
                        let second_arg = args_list.get_ptr(1).expect("no RHS to ==>");
                        let mut want_score_func = PgNodeFactory::makeFuncExpr();
                        let mut func_args = PgList::<pg_sys::Node>::new();
                        func_args.push(second_arg);

                        want_score_func.funcid = context.zdb_want_score_oid;
                        want_score_func.args = func_args.into_pg();
                        want_score_func.funcresulttype = context.zdbquery_oid;

                        // replace the second argument with our new want_score_func expression
                        args_list.pop();
                        args_list.push(want_score_func.into_pg() as *mut pg_sys::Node);
                    } else {
                        panic!("Left-hand side of ==> must be a table reference")
                    }
                }
                None => {
                    // noop
                }
            }
        }
    } else if is_a(node, pg_sys::NodeTag_T_RangeTblEntry) {
        // allow range_table_walker to continue
        return false;
    } else if is_a(node, pg_sys::NodeTag_T_Query) {
        return pg_sys::query_tree_walker(
            node as *mut pg_sys::Query,
            Some(rewrite_walker),
            context_ptr,
            pg_sys::QTW_EXAMINE_RTES as i32,
        );
    }

    return pg_sys::expression_tree_walker(node, Some(rewrite_walker), context_ptr);
}
