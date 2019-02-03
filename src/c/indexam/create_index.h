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

#ifndef __ZOMBODB_CREATE_INDEX_H__
#define __ZOMBODB_CREATE_INDEX_H__

#include "zombodb.h"

ObjectAddress
zdbDefineIndex(Oid relationId,
			   IndexStmt *stmt,
			   Oid indexRelationId,
			   bool is_alter_table,
			   bool check_rights,
			   bool check_not_in_use,
			   bool skip_build,
			   bool quiet);

#endif /* __ZOMBODB_CREATE_INDEX_H__ */
