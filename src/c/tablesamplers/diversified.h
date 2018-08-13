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

#ifndef __ZDB_DIVERSIFIED_H__
#define __ZDB_DIVERSIFIED_H__

#include "zombodb.h"
#include "nodes/relation.h"

void diversified_BeginSampleScan(SampleScanState *node, Datum *params, int nparams, uint32 seed);

#endif /* __ZDB_DIVERSIFIED_H__ */
