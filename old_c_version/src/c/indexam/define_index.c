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

#include "define_index.h"

#if (IS_PG_10)
ObjectAddress
pgDefineIndex(Oid relationId,
            IndexStmt *stmt,
            Oid indexRelationId,
            bool is_alter_table,
            bool check_rights,
            bool check_not_in_use,
            bool skip_build,
            bool quiet);
#include "indexam/indexcmds_pg10.c.inc"
#elif (IS_PG_11)
ObjectAddress
pgDefineIndex(Oid relationId,
              IndexStmt *stmt,
              Oid indexRelationId,
              Oid parentIndexId,
              Oid parentConstraintId,
              bool is_alter_table,
              bool check_rights,
              bool check_not_in_use,
              bool skip_build,
              bool quiet);
#include "indexam/indexcmds_pg11.c.inc"
#endif


ObjectAddress
zdbDefineIndex(Oid relationId,
               IndexStmt *stmt,
               Oid indexRelationId,
               Oid parentIndexId,
               Oid parentConstraintId,
               bool is_alter_table,
               bool check_rights,
               bool check_not_in_use,
               bool skip_build,
               bool quiet)
{

#if (IS_PG_10)
    return pgDefineIndex(relationId, stmt, indexRelationId, is_alter_table, check_rights, check_not_in_use, skip_build, quiet);
    /* appease compiler */
    (void) parentIndexId;
    (void) parentConstraintId;
#elif (IS_PG_11)
    return pgDefineIndex(relationId, stmt, indexRelationId, parentIndexId, parentConstraintId, is_alter_table,
            check_rights, check_not_in_use, skip_build, quiet);
#endif

}
