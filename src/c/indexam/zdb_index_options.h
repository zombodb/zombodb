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

#ifndef __ZDB_ZDB_INDEX_OPTIONS_H__
#define __ZDB_ZDB_INDEX_OPTIONS_H__

#include "postgres.h"
#include "utils/relcache.h"

#define ZDB_STRATEGY_SINGLE 1
#define ZDB_STRATEGY_ARRAY_SHOULD 2
#define ZDB_STRATEGY_ARRAY_MUST 3
#define ZDB_STRATEGY_ARRAY_NOT 4

typedef struct {
	int32 vl_len_;
	/* varlena header (do not touch directly!) */
	int   urlValueOffset;
	int   typeNameValueOffset;
	int   refreshIntervalOffset;
	int   shards;
	int   replicas;
	int   bulk_concurrency;
	int   batch_size;
	int   compressionLevel;
	int   aliasOffset;
	int   uuidOffset;
	int   optimizeAfter;
	bool  llapi;
} ZDBIndexOptions;

#define ZDBIndexOptionsGetUrlMacro(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) (relation)->rd_options)->urlValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) (relation)->rd_options) + ((ZDBIndexOptions *) (relation)->rd_options)->urlValueOffset : ("default"))

/* defined in zdbam.c */
extern char *zdb_default_elasticsearch_url_guc;
extern int  zdb_default_row_estimation_guc;
extern int  zdb_default_replicas_guc;

static inline char *ZDBIndexOptionsGetUrl(Relation rel) {
	char *url = ZDBIndexOptionsGetUrlMacro(rel);

	url = strcmp("default", url) == 0 ? zdb_default_elasticsearch_url_guc : url;
	if (url == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Cannot determine base Elasticsearch url")));
	}
	return url;
}

#define ZDBIndexOptionsGetTypeName(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) (relation)->rd_options)->typeNameValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) (relation)->rd_options) + ((ZDBIndexOptions *) (relation)->rd_options)->typeNameValueOffset : ("doc"))

#define ZDBIndexOptionsGetRefreshInterval(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) (relation)->rd_options)->refreshIntervalOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) (relation)->rd_options) + ((ZDBIndexOptions *) (relation)->rd_options)->refreshIntervalOffset : ("-1"))

#define ZDBIndexOptionsGetNumberOfShards(relation) \
    ((uint32) ((relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->shards : 5))

#define ZDBIndexOptionsGetNumberOfReplicas(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->replicas : zdb_default_replicas_guc

#define ZDBIndexOptionsGetBulkConcurrency(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->bulk_concurrency : 12

#define ZDBIndexOptionsGetBatchSize(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->batch_size : (8*1024*1024)

#define ZDBIndexOptionsGetCompressionLevel(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->compressionLevel : 1

#define ZDBIndexOptionsGetAlias(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) (relation)->rd_options)->aliasOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) (relation)->rd_options) + ((ZDBIndexOptions *) (relation)->rd_options)->aliasOffset : (NULL))

#define ZDBIndexOptionsGetIndexName(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) (relation)->rd_options)->uuidOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) (relation)->rd_options) + ((ZDBIndexOptions *) (relation)->rd_options)->uuidOffset : (NULL))

#define ZDBIndexOptionsGetLLAPI(relation) \
    ((bool) ((relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->llapi : false))

#define ZDBIndexOptionsGetOptimizeAfter(relation) \
    ((uint64) ((relation)->rd_options ? ((ZDBIndexOptions *) (relation)->rd_options)->optimizeAfter : 0))

#endif /* __ZDB_ZDB_INDEX_OPTIONS_H__ */
