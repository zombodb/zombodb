/**
 * Copyright 2018 ZomboDB, LLC
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

#include "highlighting.h"

#include "access/xact.h"
#include "catalog/index.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_func.h"

typedef struct HighlightWalkerContext {
	Oid  indexRelId;
	Oid  funcOid;
	List *highlights;
} HighlightWalkerContext;

typedef struct ZDBHighlightSupportData {
	Oid  heapOid;
	List *callbacks;
	List *callback_data;
} ZDBHighlightSupportData;

extern List *currentQueryStack;

static List *highlightEntries = NULL;


PG_FUNCTION_INFO_V1(zdb_highlight);

/*lint -esym 715,event,arg */
static void highlight_cleanup_callback(XactEvent event, void *arg) {
	highlight_support_cleanup();
}

void highlight_support_init(void) {
	RegisterXactCallback(highlight_cleanup_callback, NULL);
}

void highlight_support_cleanup(void) {
	highlightEntries = NULL;
}


static bool find_highlights_walker(PlanState *planState, HighlightWalkerContext *context) {
	Plan     *plan;
	ListCell *lc;

	if (planState == NULL)
		return false;

	plan = planState->plan;

	foreach (lc, plan->targetlist) {
		TargetEntry *te = lfirst(lc);

		if (IsA(te->expr, FuncExpr)) {
			FuncExpr *funcExpr = (FuncExpr *) te->expr;

			if (funcExpr->funcid == context->funcOid) {
				if (list_length(funcExpr->args) == 3) {
					Node             *first  = lfirst(list_head(funcExpr->args));
					Node             *second = lsecond(funcExpr->args);
					Node             *third  = lthird(funcExpr->args);
					ZDBHighlightInfo *info   = palloc0(sizeof(ZDBHighlightInfo));

					if (IsA(first, Var)) {
						Var *var = (Var *) first;
						if (var->vartype == TIDOID) {
							QueryDesc     *currentQuery = linitial(currentQueryStack);
							RangeTblEntry *rentry       = rt_fetch(var->varnoold, currentQuery->plannedstmt->rtable);

							if (rentry->relid != IndexGetRelation(context->indexRelId, false)) {
								/* it's not for the table we're looking for */
								return false;
							}

						} else {
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
											errmsg("first argument to zdb_highlight() must be a 'ctid' column")));
						}
					} else {
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
										errmsg("first argument to zdb_highlight() must be a 'ctid' column")));
					}

					if (IsA(second, Const)) {
						Const *arg = (Const *) second;
						if (arg->consttype == NAMEOID) {
							info->name = DatumGetName(arg->constvalue)->data;
						} else {
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
											errmsg("second argument to zdb_highlight() must be of type 'text'")));
						}
					} else {
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
										errmsg("second argument to zdb_highlight() must be a constant value of type 'text'")));
					}

					if (IsA(third, Const)) {
						Const *arg = (Const *) third;
						if (arg->consttype == JSONOID) {
							info->json = TextDatumGetCString(arg->constvalue);
						} else {
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
											errmsg("third argument to zdb_highlight() must be of type 'json'")));
						}
					} else {
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
										errmsg("third argument to zdb_highlight() must be a constant value of type 'json'")));
					}


					context->highlights = lappend(context->highlights, info);

				} else {
					ereport(ERROR,
							(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
									errmsg("zdb_highlight() function must have exactly three arguments")));
				}
			}
		}
	}


	return planstate_tree_walker(planState, find_highlights_walker, context);
}

List *extract_highlight_info(Oid indexRelId) {
	QueryDesc              *queryDesc = (QueryDesc *) linitial(currentQueryStack);
	HighlightWalkerContext context;
	Oid                    argtypes[] = {TIDOID, NAMEOID, JSONOID};

	context.indexRelId = indexRelId;
	context.funcOid    = LookupFuncName(lappend(lappend(NIL, makeString("zdb")), makeString("highlight")), 3, argtypes,
										false);
	context.highlights = NULL;

	find_highlights_walker(queryDesc->planstate, &context);

	return context.highlights;
}

void save_highlights(HTAB *hash, ItemPointer ctid, zdb_json_object highlights) {
	JsonObjectKeyIterator itr;

	if (highlights == NULL)
		return;

	for (itr = get_json_object_key_iterator(highlights); itr != NULL; itr = get_next_from_json_object_iterator(itr)) {
		const char        *field   = get_key_from_json_object_iterator(itr);
		size_t            fieldlen = strlen(field);
		void              *value   = get_value_from_json_object_iterator(itr);
		ZDBHighlightKey   key;
		ZDBHighlightEntry *entry;
		bool              found;
		List              *list    = NULL;
		int               len      = get_json_array_length(value);
		int               i;

		for (i = 0; i < len; i++) {
			list = lappend(list, (void *) pstrdup(get_json_array_element_string(value, i, CurrentMemoryContext)));
		}

		assert(ctid != NULL);

		memset(&key, 0, sizeof(ZDBHighlightKey));
		ItemPointerCopy(ctid, &key.ctid);
		memcpy(&key.field, field, Min(fieldlen, NAMEDATALEN));

		entry = hash_search(hash, &key, HASH_ENTER, &found);
		entry->highlights = list;
	}
}

HTAB *highlight_create_lookup_table(MemoryContext memoryContext, char *name) {
	HASHCTL ctl;

	memset(&ctl, 0, sizeof(HASHCTL));
	ctl.hcxt      = memoryContext;
	ctl.keysize   = sizeof(ZDBHighlightKey);
	ctl.entrysize = sizeof(ZDBHighlightEntry);
	ctl.hash      = tag_hash;

	return hash_create(name, 10000, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

void highlight_register_callback(Oid heapOid, highlight_lookup_callback callback, void *callback_data, MemoryContext memoryContext) {
	MemoryContext           oldContext = MemoryContextSwitchTo(memoryContext);
	ZDBHighlightSupportData *entry;
	ListCell                *lc;

	foreach(lc, highlightEntries) {
		ZDBHighlightSupportData *existing = lfirst(lc);
		if (heapOid == existing->heapOid) {
			/* we already have an entry for this name, so add another callback */
			existing->callbacks     = lappend(existing->callbacks, callback);
			existing->callback_data = lappend(existing->callback_data, callback_data);

			MemoryContextSwitchTo(oldContext);
			return;
		}
	}

	/* create a new entry */
	entry = palloc0(sizeof(ZDBHighlightSupportData));
	entry->heapOid       = heapOid;
	entry->callbacks     = lappend(entry->callbacks, callback);
	entry->callback_data = lappend(entry->callback_data, callback_data);

	highlightEntries = lappend(highlightEntries, entry);
	MemoryContextSwitchTo(oldContext);
}

static Datum highlight_lookup_highlights(Oid heapOid, ItemPointer ctid, Name field) {
	ListCell        *lc, *lc2, *lc3;
	ArrayBuildState *astate = initArrayResult(TEXTOID, CurrentMemoryContext, false);

	foreach(lc, highlightEntries) {
		ZDBHighlightSupportData *entry = lfirst(lc);

		if (heapOid == entry->heapOid) {
			forboth(lc2, entry->callbacks, lc3, entry->callback_data) {
				highlight_lookup_callback callback = lfirst(lc2);
				void                      *arg     = lfirst(lc3);
				List                      *highlights;
				ListCell                  *lc4;

				highlights = callback(ctid, field, arg);
				foreach (lc4, highlights) {
					char *hl = lfirst(lc4);
					astate = accumArrayResult(astate, CStringGetTextDatum(hl), false, TEXTOID, CurrentMemoryContext);
				}
			}
		}
	}

	return makeArrayResult(astate, CurrentMemoryContext);
}


Datum zdb_highlight(PG_FUNCTION_ARGS) {
	ItemPointer ctid      = (ItemPointer) PG_GETARG_POINTER(0);
	Name        field     = PG_GETARG_NAME(1);
	FuncExpr    *funcExpr = (FuncExpr *) fcinfo->flinfo->fn_expr;
	Node        *firstArg = linitial(funcExpr->args);

	if (IsA(firstArg, Var)) {
		Var           *var          = (Var *) firstArg;
		QueryDesc     *currentQuery = linitial(currentQueryStack);
		RangeTblEntry *rentry       = rt_fetch(var->varnoold, currentQuery->plannedstmt->rtable);

		PG_RETURN_DATUM(highlight_lookup_highlights(rentry->relid, ctid, field));
	} else {
		elog(ERROR, "zdb_highlight()'s first argument is not a direct table ctid column reference");
	}
}
