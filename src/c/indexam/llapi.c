/**
 * Copyright 2018-2019 ZomboDB, LLC
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
#include "indexam/zdbam.h"

#include "access/transam.h"
#include "access/xact.h"

PG_FUNCTION_INFO_V1(llapi_direct_insert);
PG_FUNCTION_INFO_V1(llapi_direct_delete);

Datum llapi_direct_insert(PG_FUNCTION_ARGS) {
	MemoryContext         oldContext  = MemoryContextSwitchTo(TopTransactionContext);
	Oid                   indexRelOid = PG_GETARG_OID(0);
	text                  *jsonArg    = PG_GETARG_TEXT_P(1);
	StringInfoData        json;
	Relation              indexRel;
	ZDBIndexChangeContext *context;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);

	if (!ZDBIndexOptionsGetLLAPI(indexRel)) {
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("To use ZomboDB's low-level API you must set llapi=true on the index")));
	}

	initStringInfo(&json);
	appendStringInfoString(&json, TextDatumGetCString(jsonArg));
	context = checkout_insert_context(indexRel, PointerGetDatum(NULL), true);
	ElasticsearchBulkInsertRow(context->esContext, NULL, &json, GetCurrentCommandId(true), InvalidCommandId,
							   convert_xid(GetCurrentTransactionId()), InvalidTransactionId);

	index_close(indexRel, AccessShareLock);

	MemoryContextSwitchTo(oldContext);
	PG_RETURN_VOID();
}

Datum llapi_direct_delete(PG_FUNCTION_ARGS) {
	MemoryContext         oldContext  = MemoryContextSwitchTo(TopTransactionContext);
	Oid                   indexRelOid = PG_GETARG_OID(0);
	char                  *id         = GET_STR(PG_GETARG_TEXT_P(1));
	Relation              indexRel;
	ZDBIndexChangeContext *context;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);

	if (!ZDBIndexOptionsGetLLAPI(indexRel)) {
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("To use ZomboDB's low-level API you must set llapi=true on the index")));
	}

	context = checkout_insert_context(indexRel, PointerGetDatum(NULL), true);
	ElasticsearchBulkUpdateTuple(context->esContext, NULL, id, GetCurrentCommandId(true),
								 convert_xid(GetCurrentTransactionId()));

	index_close(indexRel, AccessShareLock);

	MemoryContextSwitchTo(oldContext);
	PG_RETURN_VOID();
}
