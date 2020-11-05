use crate::utils::lookup_function;
use pgx::*;

pub struct PlanWalker {
    zdbquery_oid: pg_sys::Oid,
    zdb_score_oid: pg_sys::Oid,
    zdb_highlight_oid: pg_sys::Oid,
    zdb_anyelement_cmp_func_oid: pg_sys::Oid,
    zdb_want_score_oid: pg_sys::Oid,
    zdb_want_highlight_oid: pg_sys::Oid,
    in_te: usize,
    in_sort: usize,
    want_scores: bool,
    highlight_definitions: Vec<*mut pg_sys::FuncExpr>,
}

impl PlanWalker {
    pub fn new() -> Self {
        let zdbquery_oid = unsafe {
            pg_sys::TypenameGetTypid(
                std::ffi::CStr::from_bytes_with_nul(b"zdbquery\0")
                    .unwrap()
                    .as_ptr(),
            )
        };

        PlanWalker {
            zdbquery_oid,
            zdb_score_oid: lookup_function(vec!["zdb", "score"], Some(vec![pg_sys::TIDOID]))
                .unwrap_or(pg_sys::InvalidOid),
            zdb_highlight_oid: lookup_function(
                vec!["zdb", "highlight"],
                Some(vec![pg_sys::TIDOID, pg_sys::TEXTOID, pg_sys::JSONOID]),
            )
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
            zdb_want_highlight_oid: lookup_function(
                vec!["zdb", "want_highlight"],
                Some(vec![zdbquery_oid, pg_sys::TEXTOID, pg_sys::JSONOID]),
            )
            .unwrap_or(pg_sys::InvalidOid),
            in_te: 0,
            in_sort: 0,
            want_scores: false,
            highlight_definitions: Vec::new(),
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
                Some(plan_walker),
                self as *mut PlanWalker as void_mut_ptr,
                pg_sys::QTW_EXAMINE_RTES as i32,
            );
        }

        self.want_scores || !self.highlight_definitions.is_empty()
    }

    fn rewrite(&mut self, query: &PgBox<pg_sys::Query>) {
        unsafe {
            pg_sys::query_tree_walker(
                query.as_ptr(),
                Some(rewrite_walker),
                self as *mut PlanWalker as void_mut_ptr,
                pg_sys::QTW_EXAMINE_RTES as i32,
            );
        }
    }
}

#[pg_guard]
unsafe extern "C" fn plan_walker(node: *mut pg_sys::Node, context_ptr: void_mut_ptr) -> bool {
    if node.is_null() {
        return false;
    }

    let mut context = PgBox::<PlanWalker>::from_pg(context_ptr as *mut PlanWalker);

    if is_a(node, pg_sys::NodeTag_T_TargetEntry) {
        context.in_te += 1;
        let rc = pg_sys::expression_tree_walker(node, Some(plan_walker), context_ptr);
        context.in_te -= 1;
        return rc;
    } else if is_a(node, pg_sys::NodeTag_T_SortBy) {
        context.in_sort += 1;
        let rc = pg_sys::expression_tree_walker(node, Some(plan_walker), context_ptr);
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
        } else if func_expr.funcid == context.zdb_highlight_oid {
            // if context.in_te > 0 {
            context.highlight_definitions.push(func_expr.as_ptr());
            // } else {
            //     panic!("zdb.highlight() can only be used as a target entry");
            // }
        }
    } else if is_a(node, pg_sys::NodeTag_T_RangeTblEntry) {
        // allow range_table_walker to continue
        return false;
    } else if is_a(node, pg_sys::NodeTag_T_Query) {
        return pg_sys::query_tree_walker(
            node as *mut pg_sys::Query,
            Some(plan_walker),
            context_ptr,
            pg_sys::QTW_EXAMINE_RTES as i32,
        );
    }

    pg_sys::expression_tree_walker(node, Some(plan_walker), context_ptr)
}

#[pg_guard]
unsafe extern "C" fn rewrite_walker(node: *mut pg_sys::Node, context_ptr: void_mut_ptr) -> bool {
    if node.is_null() {
        return false;
    }

    let context = PgBox::<PlanWalker>::from_pg(context_ptr as *mut PlanWalker);

    if is_a(node, pg_sys::NodeTag_T_OpExpr) {
        let opexpr = PgBox::from_pg(node as *mut pg_sys::OpExpr);

        if opexpr.opfuncid == context.zdb_anyelement_cmp_func_oid {
            // it's our ==> operator
            let mut args_list = PgList::<pg_sys::Node>::from_pg(opexpr.args);
            let first_arg = args_list.get_ptr(0);

            if let Some(first_arg) = first_arg {
                if is_a(first_arg, pg_sys::NodeTag_T_Var)
                    || is_a(first_arg, pg_sys::NodeTag_T_FuncExpr)
                {
                    if context.want_scores {
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
                    }

                    if !context.highlight_definitions.is_empty() {
                        for definition in context.highlight_definitions.iter() {
                            // wrap the right-hand-side of the ==> operator in zdb.want_highlight
                            let second_arg = args_list.get_ptr(1).expect("no RHS to ==>");
                            let mut want_highlight_func = PgNodeFactory::makeFuncExpr();
                            let mut func_args = PgList::<pg_sys::Node>::new();
                            func_args.push(second_arg);

                            let definition = PgBox::from_pg(*definition);
                            let definition_args = PgList::from_pg(definition.args);

                            func_args.push(
                                definition_args
                                    .get_ptr(1)
                                    .expect("no field name for zdb.highlight()")
                                    as *mut pg_sys::Node,
                            );

                            // if we have a highlight definition we'll use it, if not, that's
                            // okay too as "want_highlight()" has a default for that argument
                            if let Some(definition_json) = definition_args.get_ptr(2) {
                                func_args.push(definition_json as *mut pg_sys::Node);
                            }

                            want_highlight_func.funcid = context.zdb_want_highlight_oid;
                            want_highlight_func.args = func_args.into_pg();
                            want_highlight_func.funcresulttype = context.zdbquery_oid;

                            // replace the second argument with our new want_score_func expression
                            args_list.pop();
                            args_list.push(want_highlight_func.into_pg() as *mut pg_sys::Node);
                        }
                    }
                } else {
                    panic!("Left-hand side of ==> must be a table reference or function")
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

    pg_sys::expression_tree_walker(node, Some(rewrite_walker), context_ptr)
}
