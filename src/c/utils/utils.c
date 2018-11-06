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

#include "zombodb.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

typedef struct {
	TransactionId last_xid;
	uint32        epoch;
} TxidEpoch;

typedef struct LimitInfo {
	IndexScanDesc desc;

	uint64 limit;
} LimitInfo;

/* defined in zdbam.c.  we use this to detect if we're opening a ZDB index or not */
extern bool zdbamvalidate(Oid opclassoid);

/* also defined in zdbam.c */
extern List *currentQueryStack;

void freeStringInfo(StringInfo si) {
	if (si != NULL) {
		pfree(si->data);
		pfree(si);
	}
}

/*
 * If the specified typeOid is an array, return its base element type,
 * else just return typeOid
 */
Oid get_base_type_oid(Oid typeOid) {
	Oid rc = get_element_type(typeOid);
	return rc == InvalidOid ? typeOid : rc;
}

/*
 * Given a composite Datum, return its TupleDescriptor
 * which will need to be released by the caller
 */
TupleDesc lookup_composite_tupdesc(Datum composite) {
	HeapTupleHeader td;
	Oid             tupType;
	int32           tupTypmod;

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType   = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);

	return lookup_rowtype_tupdesc(tupType, tupTypmod);
}

bool tuple_desc_contains_json(TupleDesc tupdesc) {
	int i;

	for (i = 0; i < tupdesc->natts; i++) {
		if (tupdesc->attrs[i]->atttypid == JSONOID) {
			return true;
		}
	}

	return false;
}

bool datum_contains_json(Datum composite) {
	TupleDesc tupdesc = lookup_composite_tupdesc(composite);
	bool      rc;

	rc = tuple_desc_contains_json(tupdesc);
	ReleaseTupleDesc(tupdesc);
	return rc;
}

/*
 * Convert a text* to a char*, trying our best not to make a copy of it
 */
char *text_to_cstring_maybe_no_copy(const text *t, int *len, text **possible_copy) {
	/* must cast away the const, unfortunately */
	text *tunpacked = pg_detoast_datum_packed((struct varlena *) t);

	*len           = (int) VARSIZE_ANY_EXHDR(tunpacked);
	*possible_copy = tunpacked;
	return VARDATA_ANY(tunpacked);
}

/*
 * Change all \r's, \n's, and \f's with a different character
 */
void replace_line_breaks(char *str, int len, char with_char) {
	int i;

	for (i = 0; i < len; i++) {
		switch (str[i]) {
			case '\r':
			case '\n':
			case '\f':
				str[i] = with_char;
				break;

			default:
				break;
		}
	}
}

/*
 * Find the last '}' in the input string and replace it
 * with a space
 */
char *strip_json_ending(char *str, int len) {
	while (--len) {
		switch (str[len]) {
			case '}':
				str[len] = ' ';
				return str;

			default:
				break;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("improper JSON format")));
}

Relation find_zombodb_index(Relation heapRel) {
	ListCell *lc;

	foreach (lc, RelationGetIndexList(heapRel)) {
		Oid      indexRelOid = lfirst_oid(lc);
		Relation indexRel    = relation_open(indexRelOid, AccessShareLock);

		if (indexRel->rd_amroutine != NULL && indexRel->rd_amroutine->amvalidate == zdbamvalidate)
			return indexRel;

		RelationClose(indexRel);
	}

	elog(ERROR, "Unable to locate zombodb index on '%s'", RelationGetRelationName(heapRel));
}

static bool find_limit_for_scan_walker(PlanState *planstate, LimitInfo *context) {
	Plan *plan;

	if (planstate == NULL)
		return false;

	plan = planstate->plan;

	if (IsA(plan, Limit)) {
		Limit      *limit      = (Limit *) plan;
		LimitState *limitState = (LimitState *) planstate;

		if (limit->limitCount != NULL && limit->limitOffset == NULL && IsA(limit->limitCount, Const)) {
			Const *lconst = (Const *) limit->limitCount;

			if (limitState->ps.lefttree->type == T_IndexScanState) {
				IndexScanState *indexScanState = (IndexScanState *) limitState->ps.lefttree;

				if (indexScanState->iss_ScanDesc == context->desc) {
					context->limit = DatumGetUInt64(lconst->constvalue);
					return true;
				}
			} else if (limitState->ps.lefttree->type == T_BitmapHeapScanState) {
				if (limitState->ps.lefttree->lefttree->type == T_BitmapIndexScanState) {
					BitmapIndexScanState *indexScanState = (BitmapIndexScanState *) limitState->ps.lefttree->lefttree;

					if (indexScanState->biss_ScanDesc == context->desc) {
						context->limit = DatumGetUInt64(lconst->constvalue);
						return true;
					}
				}
			}
		}
	}

	return planstate_tree_walker(planstate, find_limit_for_scan_walker, context);
}

uint64 find_limit_for_scan(IndexScanDesc scan) {
	QueryDesc *currentQuery = linitial(currentQueryStack);
	LimitInfo li;

	li.limit = 0;
	li.desc  = scan;

	find_limit_for_scan_walker(currentQuery->planstate, &li);
	return li.limit;
}

/* adapted from Postgres' txid.c#convert_xid function */
uint64 convert_xid(TransactionId xid) {
	TxidEpoch state;
	uint64    epoch;

	GetNextXidAndEpoch(&state.last_xid, &state.epoch);

	/* return special xid's as-is */
	if (!TransactionIdIsNormal(xid))
		return (uint64) xid;

	/* xid can be on either side when near wrap-around */
	epoch = (uint64) state.epoch;
	if (xid > state.last_xid && TransactionIdPrecedes(xid, state.last_xid))
		epoch--;
	else if (xid < state.last_xid && TransactionIdFollows(xid, state.last_xid))
		epoch++;

	return (epoch << 32) | xid;
}

char **array_to_strings(ArrayType *array, int *many) {
	char  **result;
	Datum *elements;
	int   nelements;
	int   i;

	Assert(ARR_ELEMTYPE(array) == TEXTOID);

	deconstruct_array(array, TEXTOID, -1, false, 'i', &elements, NULL, &nelements);

	result = (char **) palloc(sizeof(char *) * nelements);
	for (i = 0; i < nelements; i++) {
		result[i] = TextDatumGetCString(elements[i]);
		if (result[i] == NULL)
			elog(ERROR, "expected text[] of non-null values");
	}

	*many = nelements;
	return result;
}

ZDBQueryType **array_to_zdbqueries(ArrayType *array, int *many) {
	Oid          typeoid = DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum("zdbquery")));
	ZDBQueryType **result;
	Datum        *elements;
	int          nelements;
	int          i;

	Assert(ARR_ELEMTYPE(array) == typeoid);

	deconstruct_array(array, typeoid, -1, false, 'i', &elements, NULL, &nelements);

	result = (ZDBQueryType **) palloc(nelements * (sizeof(ZDBQueryType *)));
	for (i = 0; i < nelements; i++) {
		result[i] = (ZDBQueryType *) DatumGetPointer(elements[i]);
		if (result[i] == NULL)
			elog(ERROR, "expected zdbquery[] of non-null values");
	}

	*many = nelements;
	return result;
}

/* wants to be in an SPI_connect() context */
char *lookup_zdb_namespace(void) {
	StringInfo sql = makeStringInfo();

	appendStringInfo(sql,
					 "select nspname from pg_namespace where oid = (select extnamespace from pg_extension where extname = 'zombodb');");
	if (SPI_execute(sql->data, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "Cannot determine ZomboDB's namespace");

	return SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
}

void create_trigger_dependency(Oid indexRelOid, Oid triggerOid) {
	ObjectAddress indexAddress;
	ObjectAddress triggerAddress;

	indexAddress.classId     = RelationRelationId;
	indexAddress.objectId    = indexRelOid;
	indexAddress.objectSubId = 0;

	triggerAddress.classId     = TriggerRelationId;
	triggerAddress.objectId    = triggerOid;
	triggerAddress.objectSubId = 0;

	recordDependencyOn(&triggerAddress, &indexAddress, DEPENDENCY_INTERNAL);
}

Oid create_trigger(char *zombodbNamespace, char *schemaname, char *relname, Oid relid, char *triggerName, char *functionName, Oid arg, int16 type) {
	CreateTrigStmt *tgstmt;
	RangeVar       *relrv = makeRangeVar(schemaname, relname, -1);
	ObjectAddress  triggerOid;
	List           *args  = NIL;

	if (arg != InvalidOid) {
		StringInfo arg_str = makeStringInfo();
		appendStringInfo(arg_str, "%u", arg);
		args = lappend(NIL, makeString(pstrdup(arg_str->data)));
		freeStringInfo(arg_str);
	}

	tgstmt = makeNode(CreateTrigStmt);
	tgstmt->trigname     = triggerName;
	tgstmt->relation     = copyObject(relrv);
	tgstmt->funcname     = list_make2(makeString(zombodbNamespace), makeString(functionName));
	tgstmt->args         = args;
	tgstmt->row          = true;
	tgstmt->timing       = TRIGGER_TYPE_BEFORE;
	tgstmt->events       = type;
	tgstmt->columns      = NIL;
	tgstmt->whenClause   = NULL;
	tgstmt->isconstraint = false;
	tgstmt->deferrable   = false;
	tgstmt->initdeferred = false;
	tgstmt->constrrel    = NULL;

	triggerOid = CreateTrigger(tgstmt, NULL, relid, InvalidOid, InvalidOid, InvalidOid, true /* tgisinternal */);

	/* Make the new trigger visible within this session */
	CommandCounterIncrement();

	return triggerOid.objectId;
}

Relation zdb_open_index(Oid indexRelId, LOCKMODE lock) {
	Relation rel = relation_open(indexRelId, lock);

	if (rel->rd_amroutine == NULL || (rel->rd_amroutine != NULL && rel->rd_amroutine->amvalidate != zdbamvalidate)) {
		char *idxname = pstrdup(RelationGetRelationName(rel));

		relation_close(rel, lock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
						errmsg("'%s' is not a ZomboDB index", idxname)));

	}

	if (ZDBIndexOptionsGetIndexName(rel) == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
						errmsg("'%s' is missing the 'uuid' property and cannot be used.", RelationGetRelationName(rel)),
						errhint("Use REINDEX to fix this problem")));
	}

	return rel;
}

TupleDesc extract_tuple_desc_from_index_expressions(IndexInfo *indexInfo) {
	TupleDesc tupdesc = NULL;
	Node *expression;

	if (indexInfo->ii_KeyAttrNumbers[0] != SelfItemPointerAttributeNumber) {
		return NULL;
	} else if (list_length(indexInfo->ii_Expressions) != 1) {
		return NULL;
	}

	expression = (Node *) linitial(indexInfo->ii_Expressions);

	switch (nodeTag(expression)) {

		case T_Var: {
			/* It's a Var, which means it's just a reference to the type represented by the table being indexed */
			Var *var = (Var *) expression;

			/* lookup the TupleDesc for the referenced Var */
			tupdesc = lookup_rowtype_tupdesc(var->vartype, var->vartypmod);
		} break;

		case T_FuncExpr: {
			FuncExpr       *funcExpr = (FuncExpr *) expression;
			TypeCacheEntry *tpe;

			/* check that the return type of this FuncExpr is a composite type */
			tpe = lookup_type_cache(funcExpr->funcresulttype, TYPECACHE_TUPDESC);
			if (tpe->typtype != 'c') {
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
								errmsg("ZomboDB index definitions that use a function must use one that "
									   "returns a composite type")));
			}

			/*
			 * remember the TupDesc from the type cache and increment its reference count while we use it
			 * during the build process
			 */
			tupdesc = tpe->tupDesc;
			IncrTupleDescRefCount(tupdesc);
		} break;

		case T_RowExpr: {
			elog(ERROR, "TODO:  Support Row Expressions in index definitions");
		} break;

		default:
			return NULL;
	}

	return tupdesc;
}

bool index_is_zdb_index(Relation indexRel) {
	return indexRel->rd_amroutine->amvalidate == zdbamvalidate;
}

List *lookup_zdb_indexes_in_namespace(Oid namespaceOid) {
	MemoryContext current_context = CurrentMemoryContext;
	MemoryContext oldContext;
	List          *oids           = NULL;
	int           rc;
	uint64        cnt;

	SPI_connect();

	rc = SPI_execute(psprintf(
			"select oid from pg_class where relnamespace = %u and relam = (select oid from pg_am where amname = 'zombodb')",
			namespaceOid), true, INT_MAX);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "Unable to lookup indexes in namespace");

	for (cnt = 0; cnt < SPI_processed; cnt++) {
		Datum d;
		bool  isnull;
		Oid   oid;

		d   = SPI_getbinval(SPI_tuptable->vals[cnt], SPI_tuptable->tupdesc, 1, &isnull);
		oid = isnull ? InvalidOid : DatumGetObjectId(d);

		if (OidIsValid(oid)) {
			oldContext = MemoryContextSwitchTo(current_context);
			oids       = lappend_oid(oids, oid);
			MemoryContextSwitchTo(oldContext);
		}
	}

	SPI_finish();

	return oids;
}

/*
 * adapted from Postgres' commands/tablecmds.c#ATExecSetRelOptions()
 */
/*lint -efunc *,* ignore entire function */
void set_index_option(Relation rel, char *key, char *value) {
	Oid            relid;
	Relation       pgclass;
	HeapTuple      tuple;
	HeapTuple      newtuple;
	Datum          datum;
	bool           isnull;
	Datum          newOptions;
	Datum          repl_val[Natts_pg_class];
	bool           repl_null[Natts_pg_class];
	bool           repl_repl[Natts_pg_class];
	static char    *validnsps[] = HEAP_RELOPT_NAMESPACES;
	AlterTableType operation    = AT_SetRelOptions;
	List           *defList     = NIL;

	defList = lappend(defList, makeDefElemExtended(NULL, key, (Node *) makeString(value), DEFELEM_SET, -1));

	pgclass = heap_open(RelationRelationId, RowExclusiveLock);

	/* Fetch heap tuple */
	relid = RelationGetRelid(rel);
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	/* Get the old reloptions */
	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
							&isnull);

	/* Generate new proposed reloptions (text array) */
	newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
									 defList, NULL, validnsps, false,
									 operation == AT_ResetRelOptions);

	/* Validate */
	switch (rel->rd_rel->relkind) {
		case RELKIND_INDEX:
			(void) index_reloptions(rel->rd_amroutine->amoptions, newOptions, true);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is not an index",
								   RelationGetRelationName(rel))));
			break;
	}


	/*
	 * All we need do here is update the pg_class row; the new options will be
	 * propagated into relcaches during post-commit cache inval.
	 */
	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (newOptions != (Datum) 0)
		repl_val[Anum_pg_class_reloptions - 1] = newOptions;
	else
		repl_null[Anum_pg_class_reloptions - 1] = true;

	repl_repl[Anum_pg_class_reloptions - 1] = true;

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgclass),
								 repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(pgclass, &newtuple->t_self, newtuple);

	InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

	heap_freetuple(newtuple);

	ReleaseSysCache(tuple);

	heap_close(pgclass, RowExclusiveLock);

	/*
	 * Bump the command counter to ensure the next subcommand in the sequence
	 * can see the changes so far
	 */
	CommandCounterIncrement();
}
