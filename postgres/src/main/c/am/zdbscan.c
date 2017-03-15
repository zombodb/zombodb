/*
 * Copyright 2015-2017 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"
#include "access/relscan.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/nodeCustom.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "parser/parsetree.h"

#include "zdbscan.h"
#include "zdb_interface.h"

typedef struct {
    CustomScanState css;
    List            *ctid_quals;        /* list of ExprState for inequality ops */
} ZDBScanState;

static void zdb_custom_scan(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static Node *zdbscan_create_scan_state(CustomScan *cscan);
static Plan *zdbscan_plan_custom_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist, List *clauses, List *custom_plans);
static void zdbscan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *zdbscan_exec(CustomScanState *node);
static void zdbscan_end(CustomScanState *node);
static void zdbscan_rescan(CustomScanState *node);
static void zdbscan_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static set_rel_pathlist_hook_type prevPathlistHook;
static CustomPathMethods          zdbscan_path_methods;
static CustomScanMethods          zdbscan_plan_methods;
static CustomExecMethods          zdbscan_exec_methods;

static Oid zdb_operator_oid = InvalidOid;

void zdb_initialize_custom_scan(void) {
    memset(&zdbscan_path_methods, 0, sizeof(zdbscan_path_methods));
    zdbscan_path_methods.CustomName     = "ZomboDB";
    zdbscan_path_methods.PlanCustomPath = zdbscan_plan_custom_path;

    memset(&zdbscan_plan_methods, 0, sizeof(zdbscan_plan_methods));
    zdbscan_plan_methods.CustomName            = "ZomboDB";
    zdbscan_plan_methods.CreateCustomScanState = zdbscan_create_scan_state;

    memset(&zdbscan_exec_methods, 0, sizeof(zdbscan_exec_methods));
    zdbscan_exec_methods.CustomName        = "ZomboDB";
    zdbscan_exec_methods.BeginCustomScan   = zdbscan_begin;
    zdbscan_exec_methods.ExecCustomScan    = zdbscan_exec;
    zdbscan_exec_methods.EndCustomScan     = zdbscan_end;
    zdbscan_exec_methods.ReScanCustomScan  = zdbscan_rescan;
    zdbscan_exec_methods.ExplainCustomScan = zdbscan_explain;

    prevPathlistHook      = set_rel_pathlist_hook;
    set_rel_pathlist_hook = zdb_custom_scan;
}

static List *extract_zdb_clauses(Expr *expr, List *clauses) {
    if (IsA(expr, OpExpr)) {
        OpExpr *op = (OpExpr *) expr;

        if (op->opno == zdb_operator_oid) {
            clauses = lappend(clauses, op);
            elog(NOTICE, "HERE");
        }
    } else if (IsA(expr, BoolExpr)) {
        BoolExpr *be = (BoolExpr *) expr;
        ListCell *lc;

        foreach (lc, be->args) {
            Expr *arg = lfirst(lc);
            clauses = extract_zdb_clauses(arg, clauses);
        }
    }

    return clauses;
}

static void zdb_custom_scan(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte) {
    List     *clauses = NIL;
    ListCell *lc;

    if (prevPathlistHook != NULL)
        prevPathlistHook(root, rel, rti, rte);

    if (zdb_operator_oid == InvalidOid) {
        zdb_operator_oid = OpernameGetOprid(list_make1(makeString("==>")), JSONOID, TEXTOID);
        if (zdb_operator_oid == InvalidOid)
            elog(ERROR, "Unable to find ZomboDB operator ==>(json, text).  Is it on the search_path?");
    }

    foreach (lc, rel->baserestrictinfo) {
        Node *n = (Node *) lfirst(lc);

        if (IsA(n, RestrictInfo)) {
            RestrictInfo *rinfo = (RestrictInfo *) n;

            clauses = extract_zdb_clauses(rinfo->clause, clauses);
        }
    }

    if (clauses != NIL) {
        CustomPath *cpath;

        cpath = makeNode(CustomPath);
        cpath->path.type       = T_CustomPath;
        cpath->path.pathtype   = T_CustomScan;
        cpath->path.parent     = rel;
        cpath->path.pathtarget = rel->reltarget;
        cpath->path.param_info = get_baserel_parampathinfo(root, rel, rel->lateral_relids);
        cpath->flags           = 0;
        cpath->custom_private  = clauses;
        cpath->methods         = &zdbscan_path_methods;

//        CTidEstimateCosts(root, rel, cpath);

        add_path(rel, &cpath->path);
    }

}

static Plan *zdbscan_plan_custom_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist, List *clauses, List *custom_plans) {
    List       *zdb_clauses = best_path->custom_private;
    CustomScan *cscan      = makeNode(CustomScan);

    cscan->flags   = best_path->flags;
    cscan->methods = &zdbscan_plan_methods;

    cscan->scan.scanrelid       = rel->relid;
    cscan->scan.plan.targetlist = tlist;
    cscan->scan.plan.qual       = extract_actual_clauses(clauses, false);
    cscan->custom_exprs         = zdb_clauses;

    elog(NOTICE, "clauses: %s", nodeToString(zdb_clauses));
    return &cscan->scan.plan;
}

static Node *zdbscan_create_scan_state(CustomScan *cscan) {
    ZDBScanState *state = palloc0(sizeof(ZDBScanState));

    NodeSetTag(state, T_CustomScanState);
    state->css.flags   = cscan->flags;
    state->css.methods = &zdbscan_exec_methods;

    return (Node *) &state->css;
}

static void zdbscan_begin(CustomScanState *node, EState *estate, int eflags) {
    ZDBScanState *state = (ZDBScanState *) node;
    CustomScan *scan = (CustomScan *) node->ss.ps.plan;
    RangeTblEntry *rte;

    rte = rt_fetch(scan->scan.scanrelid, estate->es_range_table);

    elog(NOTICE, "beginScan");
}

static TupleTableSlot *zdbscan_exec(CustomScanState *node) {
    elog(NOTICE, "exec");
    return NULL;
}

static void zdbscan_end(CustomScanState *node) {
    elog(NOTICE, "endScan");
}

static void zdbscan_rescan(CustomScanState *node) {
    elog(NOTICE, "reScan");
}

static void zdbscan_explain(CustomScanState *node, List *ancestors, ExplainState *es) {
    elog(NOTICE, "explain");
}
