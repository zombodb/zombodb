use crate::utils::lookup_function;
use pgx::*;
use std::collections::HashSet;

struct WalkContext {
    funcoid: pg_sys::Oid,
    targetlists: Vec<*mut pg_sys::List>,
    replacements: HashSet<pg_sys::Oid>,
}

type NodePtr = *mut pg_sys::Node;

pub fn rewrite_opexrs(plan: *mut pg_sys::PlannedStmt) {
    let zdbquery_oid =
        unsafe { pg_sys::TypenameGetTypid(std::ffi::CString::new("zdbquery").unwrap().as_ptr()) };
    let funcoid = lookup_function(
        vec!["zdb", "anyelement_cmpfunc"],
        Some(vec![pg_sys::ANYELEMENTOID, zdbquery_oid]),
    )
    .unwrap_or(pg_sys::InvalidOid);

    unsafe {
        let mut context = WalkContext {
            funcoid,
            targetlists: Vec::new(),
            replacements: HashSet::new(),
        };

        walk_node(plan as NodePtr, &mut context);

        if !context.replacements.is_empty() {
            for telist in context.targetlists {
                let telist = PgList::<pg_sys::TargetEntry>::from_pg(telist);

                for te in telist.iter_ptr() {
                    let te = PgBox::from_pg(te);
                    let expr = te.expr;
                    if is_a(expr as NodePtr, pg_sys::NodeTag_T_Var) {
                        let mut var = PgBox::from_pg(expr as *mut pg_sys::Var);
                        let key = var.vartype;

                        if context.replacements.contains(&key) {
                            if var.varattno == 0 {
                                var.varattno = -1;
                            }
                            var.vartype = pg_sys::TIDOID;
                            var.varoattno = -1;
                        }
                    }
                }
            }
        }
    }
}

unsafe fn walk_plan(plan: *mut pg_sys::Plan, context: &mut WalkContext) {
    if plan.is_null() {
        return;
    }

    let plan = PgBox::from_pg(plan);
    context.targetlists.push(plan.targetlist);
    walk_node(plan.targetlist as NodePtr, context);
    walk_node(plan.initPlan as NodePtr, context);
    walk_node(plan.lefttree as NodePtr, context);
    walk_node(plan.righttree as NodePtr, context);
    walk_node(plan.qual as NodePtr, context);
}

unsafe fn walk_node(node: NodePtr, context: &mut WalkContext) {
    check_for_interrupts!();

    if node.is_null() {
        return;
    }

    if is_a(node, pg_sys::NodeTag_T_PlannedStmt) {
        let stmt = PgBox::from_pg(node as *mut pg_sys::PlannedStmt);
        walk_node(stmt.planTree as NodePtr, context);
        walk_node(stmt.subplans as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_SeqScan) {
        let mut seqscan = PgBox::from_pg(node as *mut pg_sys::SeqScan);
        walk_plan(&mut seqscan.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_IndexScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::IndexScan);
        walk_plan(&mut scan.scan.plan, context);
        walk_node(scan.indexqual as NodePtr, context);
        walk_node(scan.indexqualorig as NodePtr, context);
        walk_node(scan.indexorderby as NodePtr, context);
        walk_node(scan.indexorderby as NodePtr, context);
        walk_node(scan.indexorderbyorig as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_IndexOnlyScan) {
        // do nothing as we don't want to rewrite index scans
    } else if is_a(node, pg_sys::NodeTag_T_FuncExpr) {
        let funcexpr = PgBox::from_pg(node as *mut pg_sys::FuncExpr);
        walk_node(funcexpr.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_Aggref) {
        let aggref = PgBox::from_pg(node as *mut pg_sys::Aggref);
        walk_node(aggref.aggdirectargs as NodePtr, context);
        walk_node(aggref.args as NodePtr, context);
        walk_node(aggref.aggorder as NodePtr, context);
        walk_node(aggref.aggdistinct as NodePtr, context);
        walk_node(aggref.aggfilter as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_ModifyTable) {
        let mut modifytable = PgBox::from_pg(node as *mut pg_sys::ModifyTable);
        walk_plan(&mut modifytable.plan, context);
        walk_node(modifytable.plans as NodePtr, context);
        walk_node(modifytable.returningLists as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_LockRows) {
        let mut lockrows = PgBox::from_pg(node as *mut pg_sys::LockRows);
        walk_plan(&mut lockrows.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_SubPlan) {
        let subplan = PgBox::from_pg(node as *mut pg_sys::SubPlan);
        walk_node(subplan.testexpr, context);
        walk_node(subplan.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_TidScan) {
        let tidscan = PgBox::from_pg(node as *mut pg_sys::TidScan);
        walk_node(tidscan.tidquals as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_Append) {
        let mut append = PgBox::from_pg(node as *mut pg_sys::Append);
        walk_plan(&mut append.plan, context);
        walk_node(append.appendplans as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_BitmapAnd) {
        let mut bitmapand = PgBox::from_pg(node as *mut pg_sys::BitmapAnd);
        walk_plan(&mut bitmapand.plan, context);
        walk_node(bitmapand.bitmapplans as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_BitmapOr) {
        let mut bitmapor = PgBox::from_pg(node as *mut pg_sys::BitmapOr);
        walk_plan(&mut bitmapor.plan, context);
        walk_node(bitmapor.bitmapplans as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_BitmapHeapScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::BitmapHeapScan);
        walk_plan(&mut scan.scan.plan, context);
        walk_node(scan.bitmapqualorig as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_BitmapIndexScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::BitmapIndexScan);
        walk_plan(&mut scan.scan.plan, context);
        walk_node(scan.indexqual as NodePtr, context);
        walk_node(scan.indexqualorig as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_CustomScan) {
        // do nothing as we don't want to rewrite custom scans
    } else if is_a(node, pg_sys::NodeTag_T_ForeignScan) {
        // do nothing as we don't want to rewrite foreign scans
    } else if is_a(node, pg_sys::NodeTag_T_SampleScan) {
        // do nothing as we don't want to rewrite sample scans
    } else if is_a(node, pg_sys::NodeTag_T_FunctionScan) {
        let mut functionscan = PgBox::from_pg(node as *mut pg_sys::FunctionScan);
        walk_plan(&mut functionscan.scan.plan, context);
        walk_node(functionscan.functions as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_GatherMerge) {
        let mut gathermerge = PgBox::from_pg(node as *mut pg_sys::GatherMerge);
        walk_plan(&mut gathermerge.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_Group) {
        let mut group = PgBox::from_pg(node as *mut pg_sys::Group);
        walk_plan(&mut group.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_Join) {
        let mut join = PgBox::from_pg(node as *mut pg_sys::Join);
        walk_plan(&mut join.plan, context);
        walk_node(join.joinqual as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_MergeAppend) {
        let mut mergeappend = PgBox::from_pg(node as *mut pg_sys::MergeAppend);
        walk_plan(&mut mergeappend.plan, context);
        walk_node(mergeappend.mergeplans as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_MergeJoin) {
        let mut mergejoin = PgBox::from_pg(node as *mut pg_sys::MergeJoin);
        walk_plan(&mut mergejoin.join.plan, context);
        walk_node(mergejoin.mergeclauses as NodePtr, context);
        walk_node(mergejoin.join.joinqual as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_NamedTuplestoreScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::NamedTuplestoreScan);
        walk_plan(&mut scan.scan.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_ProjectSet) {
        let mut projectset = PgBox::from_pg(node as *mut pg_sys::ProjectSet);
        walk_plan(&mut projectset.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_RecursiveUnion) {
        let mut union = PgBox::from_pg(node as *mut pg_sys::RecursiveUnion);
        walk_plan(&mut union.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_SetOp) {
        let mut setop = PgBox::from_pg(node as *mut pg_sys::SetOp);
        walk_plan(&mut setop.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_SubqueryScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::SubqueryScan);
        walk_plan(&mut scan.scan.plan, context);
        walk_plan(scan.subplan, context);
    } else if is_a(node, pg_sys::NodeTag_T_TableFuncScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::TableFuncScan);
        walk_plan(&mut scan.scan.plan, context);
        walk_node(scan.tablefunc as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_TableFunc) {
        let tablefunc = PgBox::from_pg(node as *mut pg_sys::TableFunc);
        walk_node(tablefunc.docexpr, context);
        walk_node(tablefunc.rowexpr, context);
        walk_node(tablefunc.colexprs as NodePtr, context);
        walk_node(tablefunc.coldefexprs as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_Unique) {
        let mut unique = PgBox::from_pg(node as *mut pg_sys::Unique);
        walk_plan(&mut unique.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_ValuesScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::ValuesScan);
        walk_plan(&mut scan.scan.plan, context);
        walk_node(scan.values_lists as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_WindowAgg) {
        let mut agg = PgBox::from_pg(node as *mut pg_sys::WindowAgg);
        walk_plan(&mut agg.plan, context);
        walk_node(agg.startOffset, context);
        walk_node(agg.endOffset, context);
    } else if is_a(node, pg_sys::NodeTag_T_WorkTableScan) {
        let mut scan = PgBox::from_pg(node as *mut pg_sys::WorkTableScan);
        walk_plan(&mut scan.scan.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_Result) {
        let mut result = PgBox::from_pg(node as *mut pg_sys::Result);
        walk_plan(&mut result.plan, context);
        walk_node(result.resconstantqual, context);
    } else if is_a(node, pg_sys::NodeTag_T_Sort) {
        let mut sort: PgBox<pg_sys::Sort> = PgBox::from_pg(node as *mut pg_sys::Sort);
        walk_plan(&mut sort.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_SortGroupClause) {
        // nothing to walk
    } else if is_a(node, pg_sys::NodeTag_T_Limit) {
        let mut limit = PgBox::from_pg(node as *mut pg_sys::Limit);
        walk_plan(&mut limit.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_CoalesceExpr) {
        let expr = PgBox::from_pg(node as *mut pg_sys::CoalesceExpr);
        walk_node(expr.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_RowExpr) {
        let expr = PgBox::from_pg(node as *mut pg_sys::RowExpr);
        walk_node(expr.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_SQLValueFunction) {
        // nothing to walk
    } else if is_a(node, pg_sys::NodeTag_T_SubscriptingRef) {
        let subscript = PgBox::from_pg(node as *mut pg_sys::SubscriptingRef);
        walk_node(subscript.refupperindexpr as NodePtr, context);
        walk_node(subscript.reflowerindexpr as NodePtr, context);
        walk_node(subscript.refexpr as NodePtr, context);
        walk_node(subscript.refassgnexpr as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_MinMaxExpr) {
        let minmax = PgBox::from_pg(node as *mut pg_sys::MinMaxExpr);
        walk_node(minmax.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_ArrayExpr) {
        let array = PgBox::from_pg(node as *mut pg_sys::ArrayExpr);
        walk_node(array.elements as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_Agg) {
        let mut agg = PgBox::from_pg(node as *mut pg_sys::Agg);
        walk_plan(&mut agg.plan, context);
        walk_node(agg.chain as NodePtr, context);
        walk_node(agg.groupingSets as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_Material) {
        let mut material = PgBox::from_pg(node as *mut pg_sys::Material);
        walk_plan(&mut material.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_Gather) {
        let mut gather = PgBox::from_pg(node as *mut pg_sys::Gather);
        walk_plan(&mut gather.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_NestLoop) {
        let mut nestloop = PgBox::from_pg(node as *mut pg_sys::NestLoop);
        walk_node(nestloop.join.joinqual as NodePtr, context);
        walk_plan(&mut nestloop.join.plan as *mut pg_sys::Plan, context);
        walk_node(nestloop.nestParams as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_HashJoin) {
        let mut join = PgBox::from_pg(node as *mut pg_sys::HashJoin);
        walk_node(join.hashclauses as NodePtr, context);
        walk_node(join.hashcollations as NodePtr, context);
        walk_node(join.hashkeys as NodePtr, context);
        walk_node(join.join.joinqual as NodePtr, context);
        walk_plan(&mut join.join.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_CteScan) {
        let mut ctescan = PgBox::from_pg(node as *mut pg_sys::CteScan);
        walk_plan(&mut ctescan.scan.plan, context);
    } else if is_a(node, pg_sys::NodeTag_T_Hash) {
        let mut hash = PgBox::from_pg(node as *mut pg_sys::Hash);
        walk_plan(&mut hash.plan, context);
        walk_node(hash.hashkeys as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_WindowFunc) {
        let windowfunc = PgBox::from_pg(node as *mut pg_sys::WindowFunc);
        walk_node(windowfunc.args as NodePtr, context);
        walk_node(windowfunc.aggfilter as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_FieldSelect) {
        let fieldselect = PgBox::from_pg(node as *mut pg_sys::FieldSelect);
        walk_node(fieldselect.arg as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_RelabelType) {
        let relabeltype = PgBox::from_pg(node as *mut pg_sys::RelabelType);
        walk_node(relabeltype.arg as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_NullTest) {
        let nulltest = PgBox::from_pg(node as *mut pg_sys::NullTest);
        walk_node(nulltest.arg as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_NestLoopParam) {
        let param = PgBox::from_pg(node as *mut pg_sys::NestLoopParam);
        walk_node(param.paramval as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_OpExpr) {
        let opexpr = PgBox::from_pg(node as *mut pg_sys::OpExpr);
        if opexpr.opfuncid == context.funcoid {
            let mut args = PgList::<pg_sys::Node>::from_pg(opexpr.args);
            if let Some(first_arg) = args.get_ptr(0) {
                if is_a(first_arg, pg_sys::NodeTag_T_Var) {
                    let mut first_arg = PgBox::from_pg(first_arg as *mut pg_sys::Var);
                    if first_arg.vartype != pg_sys::TIDOID {
                        context.replacements.insert(first_arg.vartype);
                        first_arg.vartype = pg_sys::TIDOID;
                        first_arg.varoattno = pg_sys::SelfItemPointerAttributeNumber as i16;
                        if first_arg.varattno == 0 {
                            first_arg.varattno = pg_sys::SelfItemPointerAttributeNumber as i16;
                        }
                    }
                } else if is_a(first_arg, pg_sys::NodeTag_T_FuncExpr) {
                    let first_arg = PgBox::from_pg(first_arg as *mut pg_sys::FuncExpr);
                    let mut var = PgNodeFactory::makeVar();
                    context.replacements.insert(first_arg.funcresulttype);
                    var.vartype = pg_sys::TIDOID;
                    var.varno = 1;
                    var.varattno = pg_sys::SelfItemPointerAttributeNumber as i16;
                    var.varoattno = pg_sys::SelfItemPointerAttributeNumber as i16;
                    var.location = first_arg.location;

                    args.replace_ptr(0, var.into_pg() as NodePtr);
                }
            }
        } else {
            walk_node(opexpr.args as NodePtr, context);
        }
    } else if is_a(node, pg_sys::NodeTag_T_TargetEntry) {
        let te = PgBox::from_pg(node as *mut pg_sys::TargetEntry);
        walk_node(te.expr as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_BoolExpr) {
        let boolexp = PgBox::from_pg(node as *mut pg_sys::BoolExpr);
        walk_node(boolexp.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_CaseExpr) {
        let case = PgBox::from_pg(node as *mut pg_sys::CaseExpr);
        walk_node(case.arg as NodePtr, context);
        walk_node(case.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_CaseWhen) {
        let when = PgBox::from_pg(node as *mut pg_sys::CaseWhen);
        walk_node(when.expr as NodePtr, context);
        walk_node(when.result as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_RangeTblFunction) {
        let rangetblfn = PgBox::from_pg(node as *mut pg_sys::RangeTblFunction);
        walk_node(rangetblfn.funcexpr as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_CoerceViaIO) {
        let coerce = PgBox::from_pg(node as *mut pg_sys::CoerceViaIO);
        walk_node(coerce.arg as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_ScalarArrayOpExpr) {
        let scalar = PgBox::from_pg(node as *mut pg_sys::ScalarArrayOpExpr);
        walk_node(scalar.args as NodePtr, context);
    } else if is_a(node, pg_sys::NodeTag_T_List) {
        let list = PgList::<pg_sys::Node>::from_pg(node as *mut pg_sys::List);
        for entry in list.iter_ptr() {
            walk_node(entry, context);
        }
    } else if is_a(node, pg_sys::NodeTag_T_OidList) {
    } else if is_a(node, pg_sys::NodeTag_T_IntList) {
    } else if is_a(node, pg_sys::NodeTag_T_Var) {
    } else if is_a(node, pg_sys::NodeTag_T_Const) {
    } else if is_a(node, pg_sys::NodeTag_T_Param) {
    } else if is_a(node, pg_sys::NodeTag_T_CaseTestExpr) {
    } else {
        warning!("unrecognized tag: {}", node.as_ref().unwrap().type_);
        eprintln!("unrecognized tag: {}", node.as_ref().unwrap().type_);
    }
}
