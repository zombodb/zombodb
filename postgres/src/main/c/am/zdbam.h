/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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
#ifndef __ZDBAM_H__
#define __ZDBAM_H__

#include "postgres.h"
#include "fmgr.h"
#include "zdb_interface.h"

extern void  zdbam_init(void);
extern void  zdbam_fini(void);
extern Datum zdbbuild(PG_FUNCTION_ARGS);
extern Datum zdbbuildempty(PG_FUNCTION_ARGS);
extern Datum zdbinsert(PG_FUNCTION_ARGS);
extern Datum zdbbeginscan(PG_FUNCTION_ARGS);
extern Datum zdbgettuple(PG_FUNCTION_ARGS);
extern Datum zdbrescan(PG_FUNCTION_ARGS);
extern Datum zdbendscan(PG_FUNCTION_ARGS);
extern Datum zdbmarkpos(PG_FUNCTION_ARGS);
extern Datum zdbrestrpos(PG_FUNCTION_ARGS);
extern Datum zdbbulkdelete(PG_FUNCTION_ARGS);
extern Datum zdbvacuumcleanup(PG_FUNCTION_ARGS);
extern Datum zdboptions(PG_FUNCTION_ARGS);
extern Datum zdbeventtrigger(PG_FUNCTION_ARGS);
extern Datum zdbcostestimate(PG_FUNCTION_ARGS);

extern Datum zdb_num_hits(PG_FUNCTION_ARGS);

#endif
