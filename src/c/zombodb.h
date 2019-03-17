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
#ifndef __ZDB_ZDB__H__
#define __ZDB_ZDB__H__

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "indexam/zdb_index_options.h"
#include "type/zdbquerytype.h"
#include "utils/utils.h"
#include <assert.h>

#define ZDB_VERSION "10-1.0.4"

#ifndef PG_GETARG_JSONB_P
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

#ifndef JsonbPGetDatum
#define JsonbPGetDatum JsonbGetDatum
#endif


#ifndef PG_GETARG_JSONB_P
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

#ifndef JsonbPGetDatum
#define JsonbPGetDatum JsonbGetDatum
#endif


#endif /* __ZDB_ZDB__H__ */
