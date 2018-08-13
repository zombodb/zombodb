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
#include "highlighting/highlighting.h"
#include "rest/curl_support.h"
#include "scoring/scoring.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(zdb_version);
PG_FUNCTION_INFO_V1(zdb_ctid);

extern void zdb_aminit(void);

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void) {
	/* these initialization functions might register transaction callbacks that
	 * run on transaction commit/abort.  If so, they're called by Postgres in a
	 * LIFO order
	 */
	curl_support_init();
	json_support_init();
	scoring_support_init();
	highlight_support_init();

	/* callbacks registered here should always be the first to run, so it's the last one we initialize */
	zdb_aminit();

	elog(LOG, "ZomboDB Loaded");
}

void _PG_fini(void) {

}

Datum zdb_version(PG_FUNCTION_ARGS) {
	return CStringGetTextDatum(ZDB_VERSION);
}

Datum zdb_ctid(PG_FUNCTION_ARGS) {
	/*lint -e732 casting to uint64 on purpose */
	uint64      ctid64 = PG_GETARG_INT64(0);
	ItemPointer ctid   = palloc(sizeof(ItemPointerData));

	/*lint -e613 ctid will never be null here */
	ItemPointerSet(ctid, (BlockNumber) (ctid64 >> 32), (OffsetNumber) ctid64);
	PG_RETURN_POINTER(ctid);
}
