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

#include "elasticsearch.h"
#include "elasticsearch/mapping.h"
#include "elasticsearch/querygen.h"
#include "highlighting/highlighting.h"
#include "rest/rest.h"
#include "indexam/zdbam.h"

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "commands/dbcommands.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"

/* an ES limit introduced around Elasticsearch v5 */
#define MAX_DOCS_PER_REQUEST 10000

#define ES_BULK_RESPONSE_FILTER "errors,items.*.error"
#define ES_SEARCH_RESPONSE_FILTER "_scroll_id,_shards.failed,hits.total,hits.hits.fields.*,hits.hits._id,hits.hits._score,hits.hits.highlight.*"

#define validate_alias(indexRel) \
    do { \
        if (ZDBIndexOptionsGetAlias((indexRel)) == NULL) \
            elog(ERROR, "index '%s' doesn't have an alias", RelationGetRelationName((indexRel))); \
    } while(0)


static PostDataEntry *checkout_batch_pool(ElasticsearchBulkContext *context) {
	int i;

	for (i = 0; i < context->rest->nhandles + 1; i++) {
		if (context->pool[i] != NULL) {
			PostDataEntry *entry = palloc(sizeof(PostDataEntry));

			entry->buff            = context->pool[i];
			entry->pool_idx        = i;
			entry->compressed_data = NULL;

			context->pool[i] = NULL;
			return entry;
		}
	}

	elog(ERROR, "Unable to checkout from batch pool");
}

char *make_alias_name(Relation indexRel, bool force_default) {

	if (!force_default && ZDBIndexOptionsGetAlias(indexRel) != NULL) {
		return ZDBIndexOptionsGetAlias(indexRel);
	} else {
		Relation heapRel = RelationIdGetRelation(IndexGetRelation(RelationGetRelid(indexRel), false));
		char     *name;

		name = psprintf("%s.%s.%s.%s-%u",
						get_database_name(MyDatabaseId),
						get_namespace_name(RelationGetNamespace(indexRel)),
						RelationGetRelationName(heapRel),
						RelationGetRelationName(indexRel),
						RelationGetRelid(indexRel));
		RelationClose(heapRel);

		return str_tolower(name, strlen(name), DEFAULT_COLLATION_OID);
	}
}

static char *generate_uuid_index_name(Relation indexRel) {
	Relation heapRel = RelationIdGetRelation(IndexGetRelation(RelationGetRelid(indexRel), false));
	char     *name;

	name = psprintf("%u.%u.%u.%u-%lu",
					MyDatabaseId,
					RelationGetNamespace(indexRel),
					RelationGetRelid(heapRel),
					RelationGetRelid(indexRel),
					(unsigned long) random());
	RelationClose(heapRel);

	return name;
}

char *ElasticsearchArbitraryRequest(Relation indexRel, char *method, char *endpoint, StringInfo postData) {
	StringInfo request = makeStringInfo();

	assert(method != NULL);

	if (endpoint[0] == '/') {
		/* caller wants to directly query the cluster from the root */
		appendStringInfo(request, "%s%s", ZDBIndexOptionsGetUrl(indexRel), endpoint + 1);
		return rest_call(method, request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel))->data;
	} else {
		/* caller wants to query the index */
		appendStringInfo(request, "%s%s/%s", ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel),
						 endpoint);
		return rest_call(method, request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel))->data;
	}
}

char *ElasticsearchCreateIndex(Relation heapRel, Relation indexRel, TupleDesc tupdesc, char *aliasName) {
	char       *indexName = generate_uuid_index_name(indexRel);
	StringInfo request    = makeStringInfo();
	StringInfo settings   = makeStringInfo();
	StringInfo mapping    = generate_mapping(heapRel, tupdesc);
	StringInfo response;

	if (ZDBIndexOptionsGetIndexName(indexRel) != NULL) {
		elog(LOG, "[zombodb] Reusing index with name '%s'", ZDBIndexOptionsGetIndexName(indexRel));
		pfree(indexName);
		indexName = pstrdup(ZDBIndexOptionsGetIndexName(indexRel));
	}

	appendStringInfo(settings, ""
							   "{"
							   "   \"settings\": {"
							   "      \"number_of_shards\": %d,"
							   "      \"index.number_of_replicas\": 0,"
							   "      \"index.refresh_interval\": \"-1\","
							   "      \"index.query.default_field\": \"zdb_all\","
							   "      \"analysis\": {"
							   "         \"filter\": { %s },"
							   "         \"char_filter\" : { %s },"
							   "         \"tokenizer\" : { %s },"
							   "         \"analyzer\": { %s },"
							   "         \"normalizer\": { %s }"
							   "      }"
							   "   },"
							   "   \"mappings\": {"
							   "      \"%s\": { "
							   "         \"_source\": { \"enabled\": true },"
							   "         \"dynamic_templates\": ["
							   "              {"
							   "                 \"strings\": {"
							   "                    \"match_mapping_type\": \"string\","
							   "                    \"mapping\": {"
							   "                       \"type\": \"keyword\","
							   "                       \"ignore_above\": 10922,"
							   "                       \"normalizer\": \"lowercase\","
							   "                       \"copy_to\": \"zdb_all\""
							   "                     }"
							   "                  }"
							   "              },"
							   "              {"
							   "                 \"dates_times\": {"
							   "                    \"match_mapping_type\": \"date\","
							   "                    \"mapping\": {"
							   "                       \"type\": \"date\","
							   "                       \"format\": \"strict_date_optional_time||epoch_millis||HH:mm:ss.SSSSSS||HH:mm:ss.SSSSSSZZ\","
							   "                       \"copy_to\": \"zdb_all\""
							   "                     }"
							   "                  }"
							   "              }"
							   "         ],"
							   "         \"_all\": {\"enabled\":false},"
							   "         \"properties\": { %s}"
							   "      }"
							   "   },"
							   "   \"aliases\": {"
							   "      \"%s\": {}"
							   "   }"
							   "}",
					 ZDBIndexOptionsGetNumberOfShards(indexRel),
					 lookup_analysis_thing(CurrentMemoryContext, "filters"),
					 lookup_analysis_thing(CurrentMemoryContext, "char_filters"),
					 lookup_analysis_thing(CurrentMemoryContext, "tokenizers"),
					 lookup_analysis_thing(CurrentMemoryContext, "analyzers"),
					 lookup_analysis_thing(CurrentMemoryContext, "normalizers"),
					 ZDBIndexOptionsGetTypeName(indexRel),
					 mapping->data,
					 aliasName);

	appendStringInfo(request, "%s%s", ZDBIndexOptionsGetUrl(indexRel), indexName);

	/* first, delete the old index */
	ElasticsearchDeleteIndex(indexRel);

	/* secondly, create the new index */
	response = rest_call("PUT", request, settings, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(mapping);
	freeStringInfo(settings);
	freeStringInfo(request);
	freeStringInfo(response);

	return indexName;
}

void ElasticsearchDeleteIndex(Relation indexRel) {
	StringInfo request = makeStringInfo();
	StringInfo response;

	appendStringInfo(request, "%s%s", ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("DELETE", request, NULL, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(request);
	freeStringInfo(response);
}

void ElasticsearchDeleteIndexDirect(char *index_url) {
	StringInfo request = makeStringInfo();
	StringInfo response;

	elog(LOG, "[ZomboDB] DELETING remote index %s", index_url);
	appendStringInfo(request, "%s", index_url);
	response = rest_call("DELETE", request, NULL, 0);

	freeStringInfo(request);
	freeStringInfo(response);
}

void ElasticsearchFinalizeIndexCreation(Relation indexRel) {
	StringInfo request  = makeStringInfo();
	StringInfo settings = makeStringInfo();
	StringInfo response;

	appendStringInfo(settings, ""
							   "{"
							   "   \"index\": {"
							   "      \"refresh_interval\": \"%s\","
							   "      \"number_of_replicas\": %d"
							   "   }"
							   "}",
					 ZDBIndexOptionsGetRefreshInterval(indexRel),
					 ZDBIndexOptionsGetNumberOfReplicas(indexRel));

	appendStringInfo(request, "%s%s/_settings", ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("PUT", request, settings, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(settings);
	freeStringInfo(request);
	freeStringInfo(response);
}

void ElasticsearchUpdateSettings(Relation indexRel, char *oldAlias, char *newAlias) {
	if (oldAlias == NULL)
		oldAlias = make_alias_name(indexRel, true);
	if (newAlias == NULL)
		newAlias = make_alias_name(indexRel, false);

	if (strcmp(oldAlias, newAlias) != 0) {
		StringInfo request  = makeStringInfo();
		StringInfo settings = makeStringInfo();
		StringInfo response;

		appendStringInfo(settings, ""
								   "{"
								   "   \"actions\": ["
								   "      {\"remove\": {\"index\": \"%s\", \"alias\":\"%s\" } },"
								   "      {\"add\": {\"index\": \"%s\", \"alias\":\"%s\" } }"
								   "   ]"
								   "}",
						 ZDBIndexOptionsGetIndexName(indexRel),
						 oldAlias,
						 ZDBIndexOptionsGetIndexName(indexRel),
						 newAlias);

		appendStringInfo(request, "%s_aliases", ZDBIndexOptionsGetUrl(indexRel));
		response = rest_call("POST", request, settings, ZDBIndexOptionsGetCompressionLevel(indexRel));

		freeStringInfo(settings);
		freeStringInfo(request);
		freeStringInfo(response);
	}

	ElasticsearchFinalizeIndexCreation(indexRel);
}

void ElasticsearchPutMapping(Relation heapRel, Relation indexRel, TupleDesc tupdesc) {
	StringInfo request  = makeStringInfo();
	StringInfo settings = makeStringInfo();
	StringInfo mapping  = generate_mapping(heapRel, tupdesc);
	StringInfo response;

	appendStringInfo(settings, ""
							   "{"
							   "   \"properties\": {%s}"
							   "}",
					 mapping->data);

	appendStringInfo(request, "%s%s/_mapping/doc", ZDBIndexOptionsGetUrl(indexRel),
					 ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("PUT", request, settings, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(settings);
	freeStringInfo(request);
	freeStringInfo(response);
}


ElasticsearchBulkContext *ElasticsearchStartBulkProcess(Relation indexRel, char *indexName, TupleDesc tupdesc, bool ignore_version_conflicts) {
	ElasticsearchBulkContext *context = palloc0(sizeof(ElasticsearchBulkContext));
	int                      i;

	if (indexName == NULL) {
		indexName = ZDBIndexOptionsGetIndexName(indexRel);
		if (indexName == NULL) {
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
							errmsg("The 'uuid' property is not set on %s", RelationGetRelationName(indexRel))));
		}
	}

	context->url                    = pstrdup(ZDBIndexOptionsGetUrl(indexRel));
	context->pgIndexName            = pstrdup(RelationGetRelationName(indexRel));
	context->esIndexName            = pstrdup(indexName);
	context->typeName               = pstrdup(ZDBIndexOptionsGetTypeName(indexRel));
	context->batchSize              = ZDBIndexOptionsGetBatchSize(indexRel);
	context->bulkConcurrency        = ZDBIndexOptionsGetBulkConcurrency(indexRel);
	context->compressionLevel       = ZDBIndexOptionsGetCompressionLevel(indexRel);
	context->shouldRefresh          = strcmp("-1", ZDBIndexOptionsGetRefreshInterval(indexRel)) == 0;
	context->ignoreVersionConflicts = ignore_version_conflicts;
	context->rest                   = rest_multi_init(context->bulkConcurrency, ignore_version_conflicts);

	for (i = 0; i < context->bulkConcurrency + 1; i++)
		context->pool[i] = makeStringInfo();

	context->rest->pool          = context->pool;
	context->current             = checkout_batch_pool(context);
	context->waitForActiveShards = false;

	if (tupdesc != NULL) {
		/*
		 * look for fields of type ::json in the tuple and note the existence
		 *
		 * As dumb as it sounds, we need to know this so we can (optionally) strip
		 * line break characters from the json/string version of the row being indexed
		 */
		context->containsJson      = tuple_desc_contains_json(tupdesc);
		context->containsJsonIsSet = true;
	}

	return context;
}

static inline void mark_transaction_in_progress(ElasticsearchBulkContext *context, TransactionId curr_xid) {
	uint64 xid = convert_xid(curr_xid);

	appendStringInfo(context->current->buff,
					 "{\"update\":{\"_id\":\"zdb_aborted_xids\",\"_retry_on_conflict\":128}}\n");
	appendStringInfo(context->current->buff,
					 "{\"upsert\":{\"zdb_aborted_xids\":[%lu]},"
					 "\"script\":{\"source\":\"ctx._source.zdb_aborted_xids.add(params.XID);\",\"lang\":\"painless\",\"params\":{\"XID\":%lu}}}\n",
					 xid, xid);

	context->nxid++;
}

static inline void remember_curr_xid(ElasticsearchBulkContext *context) {
	TransactionId curr_xid = GetCurrentTransactionId();

	if (context->lastUsedXid != curr_xid) {
		if (!list_member_int(context->usedXids, curr_xid)) {
			/*
			 * we haven't seen this transaction id yet, so remember it and also mark
			 * it as in-progress in the index
			 */
			MemoryContext oldContext;

			oldContext = MemoryContextSwitchTo(TopTransactionContext);
			context->usedXids    = lappend_int(context->usedXids, curr_xid);
			context->lastUsedXid = curr_xid;
			MemoryContextSwitchTo(oldContext);

			mark_transaction_in_progress(context, curr_xid);
		}
	}
}

static inline void bulk_prologue(ElasticsearchBulkContext *context, bool is_final) {
	if (rest_multi_perform(context->rest))
		rest_multi_partial_cleanup(context->rest, false, true);

	if (!is_final)
		remember_curr_xid(context);

	if (context->current->buff->len >= context->batchSize || context->nrows == MAX_DOCS_PER_REQUEST || is_final) {
		StringInfo request = makeStringInfo();

		if (!is_final) {
			elog(ZDB_LOG_LEVEL,
				 "[zombodb] processed %d rows in %s (nbytes=%d, nrows=%d, active=%d of %d)",
				 context->ntotal,
				 context->pgIndexName,
				 context->current->buff->len,
				 context->nrows,
				 context->bulkConcurrency - context->rest->available,
				 context->bulkConcurrency);
		}

		appendStringInfo(request, "%s%s/%s/_bulk?filter_path=%s", context->url, context->esIndexName, context->typeName,
						 ES_BULK_RESPONSE_FILTER);
		if (context->waitForActiveShards)
			appendStringInfo(request, "&wait_for_active_shards=all");

		if (is_final && context->shouldRefresh && context->nrequests == 0)
			appendStringInfo(request, "&refresh=true");

		rest_multi_call(context->rest, "POST", request, context->current, context->compressionLevel);
		freeStringInfo(request);

		context->nrows = 0;
		context->nrequests++;

		if (!is_final) {
			context->current = checkout_batch_pool(context);
		}
	}
}

static inline void bulk_epilogue(ElasticsearchBulkContext *context) {
	context->nrows++;
	context->ntotal++;
}

void ElasticsearchBulkInsertRow(ElasticsearchBulkContext *context, ItemPointerData *ctid, text *json, CommandId cmin, CommandId cmax, uint64 xmin, uint64 xmax) {
	text *possible_copy;
	int  len;
	char *as_string;

	bulk_prologue(context, false);

	/* convert the input json text into a c-string */
	as_string = text_to_cstring_maybe_no_copy(json, &len, &possible_copy);

	/*
	 * ES' _bulk endpoint requires that the document json be on a single line.
	 * In general, Postgres' row_to_json() function will have already done this for us, but
	 * if the row contains a field of type ::json, that'll be encoded as-is, and
	 * that means in that case we need to find and replace line breaks with spaces
	 */
	if (context->containsJson)
		replace_line_breaks(as_string, len, ' ');

	/*
	 * The first line is telling Elasticsearch that we intend to index a document.
	 *
	 * We don't specify _index or _type because they're already in our request URL,
	 * and we don't specify an _id because we let Elasticsearch autogenerate one for us --
	 * we'll never use the _id for ourselves, so we don't care what it is
	 */
	if (ctid != NULL) {
		appendStringInfo(context->current->buff, "{\"index\":{\"_id\":\"%lu\"}}\n", ItemPointerToUint64(ctid));
	} else {
		appendStringInfo(context->current->buff, "{\"index\":{}}\n");
	}

	/* the second line is the json form of the document... */
	appendBinaryStringInfo(context->current->buff, strip_json_ending(as_string, len), len);

	if (ctid != NULL) {
		/* ...but we tack on our zdb_ctid property */
		appendStringInfo(context->current->buff, ",\"zdb_ctid\":%lu", ItemPointerToUint64(ctid));
	}

	/* ...and cmin/cmax */
	appendStringInfo(context->current->buff, ",\"zdb_cmin\":%u", cmin);
	if (cmax != InvalidCommandId)
		appendStringInfo(context->current->buff, ",\"zdb_cmax\":%u", cmax);

	/* ...and xmin/xmax */
	appendStringInfo(context->current->buff, ",\"zdb_xmin\":%lu", xmin);
	if (xmax != InvalidTransactionId)
		appendStringInfo(context->current->buff, ",\"zdb_xmax\":%lu", xmax);

	appendStringInfo(context->current->buff, "}\n");

	if (possible_copy != json)
		pfree(possible_copy);
	pfree(json);

	context->nindex++;
	bulk_epilogue(context);
}

void ElasticsearchBulkUpdateTuple(ElasticsearchBulkContext *context, ItemPointer ctid, char *llapi_id, CommandId cmax, uint64 xmax) {
	bulk_prologue(context, false);

	if (ctid != NULL) {
		appendStringInfo(context->current->buff, "{\"update\":{\"_id\":\"%lu\",\"_retry_on_conflict\":1}}\n",
						 ItemPointerToUint64(ctid));
	} else {
		appendStringInfo(context->current->buff, "{\"update\":{\"_id\":\"%s\",\"_retry_on_conflict\":1}}\n", llapi_id);
	}
	appendStringInfo(context->current->buff,
					 "{\"script\":{\"source\":\""
					 "ctx._source.zdb_cmax=params.CMAX;"
					 "ctx._source.zdb_xmax=params.XMAX;\",\"lang\":\"painless\",\"params\":{\"CMAX\":%u,\"XMAX\":%lu}}}\n",
					 cmax, xmax);

	context->nupdate++;
	bulk_epilogue(context);
}

void ElasticsearchBulkVacuumXmax(ElasticsearchBulkContext *context, char *_id, uint64 expected_xmax) {
	bulk_prologue(context, false);

	appendStringInfo(context->current->buff, "{\"update\":{\"_id\":\"%s\",\"_retry_on_conflict\":0}}\n", _id);
	appendStringInfo(context->current->buff,
					 "{\"script\":{\"source\":\""
					 "if (ctx._source.zdb_xmax != params.EXPECTED_XMAX) {"
					 "   ctx.op='none';"
					 "} else {"
					 "   ctx._source.zdb_xmax=null;"
					 "}\",\"lang\":\"painless\",\"params\":{\"EXPECTED_XMAX\":%lu}}}\n",
					 expected_xmax);

	context->nvacuum++;
	bulk_epilogue(context);
}

void ElasticsearchBulkDeleteRowByXmin(ElasticsearchBulkContext *context, char *_id, uint64 xmin) {
	/* important to tag this before we do the work in bulk_prologue() */
	context->waitForActiveShards = true;

	bulk_prologue(context, false);

	appendStringInfo(context->current->buff, "{\"update\":{\"_id\":\"%s\"}}\n", _id);
	appendStringInfo(context->current->buff,
					 "{\"script\":{\"source\":\""
					 "if (ctx._source.zdb_xmin == params.EXPECTED_XMIN) {"
					 "   ctx.op='delete';"
					 "} else {"
					 "   ctx.op='none';"
					 "}\",\"lang\":\"painless\",\"params\":{\"EXPECTED_XMIN\":%lu}}}\n",
					 xmin);

	context->ndelete++;
	bulk_epilogue(context);
}

void ElasticsearchBulkDeleteRowByXmax(ElasticsearchBulkContext *context, char *_id, uint64 xmax) {
	/* important to tag this before we do the work in bulk_prologue() */
	context->waitForActiveShards = true;

	bulk_prologue(context, false);

	appendStringInfo(context->current->buff, "{\"update\":{\"_id\":\"%s\"}}\n", _id);
	appendStringInfo(context->current->buff,
					 "{\"script\":{\"source\":\""
					 "if (ctx._source.zdb_xmax == params.EXPECTED_XMAX) {"
					 "   ctx.op='delete';"
					 "} else {"
					 "   ctx.op='none';"
					 "}\",\"lang\":\"painless\",\"params\":{\"EXPECTED_XMAX\":%lu}}}\n",
					 xmax);

	context->ndelete++;
	bulk_epilogue(context);
}

static void mark_transaction_committed(ElasticsearchBulkContext *context, TransactionId which_xid) {
	uint64 xid = convert_xid(which_xid);

	appendStringInfo(context->current->buff,
					 "{\"update\":{\"_id\":\"zdb_aborted_xids\",\"_retry_on_conflict\":128}}\n");
	appendStringInfo(context->current->buff, ""
											 "{"
											 "\"script\":{"
											 "\"source\":\"ctx._source.zdb_aborted_xids.remove(ctx._source.zdb_aborted_xids.indexOf(params.XID));\","
											 "\"params\":{\"XID\":%lu},"
											 "\"lang\":\"painless\""
											 "}"
											 "}\n", xid);
	context->nxid++;
}

void ElasticsearchFinishBulkProcess(ElasticsearchBulkContext *context, bool is_commit) {
	StringInfo request  = makeStringInfo();
	bool       did_xids = false;

	if (is_commit) {
		if (context->rest->available == context->rest->nhandles) {
			/*
			 * we don't have any active requests, so if we can fit
			 * all our used_xids into the current bulk request, then do so
			 */
			if (context->nindex + context->nupdate + context->nxid + list_length(context->usedXids) <
				MAX_DOCS_PER_REQUEST) {
				/* the fist, so we can add them here to mark the used transaction ids as committed */
				ListCell *lc;

				foreach (lc, context->usedXids) {
					mark_transaction_committed(context, (TransactionId) lfirst_int(lc));
				}

				did_xids = true;
			} else {
				/* we can't mark them as committed now, but we will below when everything else is done
				 * so go ahead and note that we're going to do another request
				 */
				context->nrequests++;
			}
		}
	}

	if (context->current->buff->len > 0) {
		/* we have more data to send to ES via curl */
		bulk_prologue(context, true);

		/* we only want to log if we required more than 1 batch */
		if (context->nrequests > 1) {
			elog(ZDB_LOG_LEVEL,
				 "[zombodb] processed %d total rows in %d batches for %s (nindex=%d, nupdate=%d, ndelete=%d, nvacuum=%d, nxid=%d)",
				 context->ntotal,
				 context->nrequests,
				 context->pgIndexName,
				 context->nindex,
				 context->nupdate,
				 context->ndelete,
				 context->nvacuum,
				 context->nxid);
		}
	}

	/* wait for all outstanding HTTP requests to finish */
	if (context->nrequests > 0) {
		rest_multi_wait_for_all_done(context->rest);
	}

	/* after this call, context->rest is no longer usable */
	rest_multi_partial_cleanup(context->rest, true, false);

	if (!is_commit) {
		/* reset the context->rest struct so that this bulk process can still be used again */
		context->rest       = rest_multi_init(context->bulkConcurrency, context->ignoreVersionConflicts);
		context->rest->pool = context->pool;
	}

	if (is_commit && !did_xids) {
		/*
		 * we couldn't mark transactions as committed above, so do it now that all outstanding
		 * multi-rest calls are finished
		 */
		StringInfo endpoint = makeStringInfo();
		ListCell   *lc;

		context->current = checkout_batch_pool(context);
		foreach (lc, context->usedXids) {
			mark_transaction_committed(context, (TransactionId) lfirst_int(lc));
		}

		appendStringInfo(endpoint, "%s%s/%s/_bulk?filter_path=%s", context->url, context->esIndexName,
						 context->typeName, ES_BULK_RESPONSE_FILTER);
		rest_call("POST", endpoint, context->current->buff, context->compressionLevel);
	}

	if (context->shouldRefresh && context->nrequests > 1) {
		/* we did more than 1 request, so force a full refresh across the entire index */
		resetStringInfo(request);
		appendStringInfo(request, "%s%s/_refresh", context->url, context->esIndexName);
		rest_call("POST", request, NULL, context->compressionLevel);
	}

	freeStringInfo(request);

	if (is_commit) {
		pfree(context->esIndexName);
		pfree(context->pgIndexName);
		pfree(context);
	}
}

uint64 ElasticsearchCountAllDocs(Relation indexRel) {
	StringInfo request  = makeStringInfo();
	StringInfo postData = makeStringInfo();
	StringInfo response;
	Datum      count;

	appendStringInfo(postData, "{\"query\":{\"match_all\":{}}}");
	appendStringInfo(request,
					 "%s%s/%s/_count?filter_path=count",
					 ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel),
					 ZDBIndexOptionsGetTypeName(indexRel));
	response = rest_call("GET", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));
	count    = DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data),
								   CStringGetTextDatum("count"));

	/* convert the count property to an int8 and return as uint64 */
	return DatumGetUInt64(DirectFunctionCall1(int8in, PointerGetDatum(TextDatumGetCString(count))));
}

uint64 ElasticsearchEstimateSelectivity(Relation indexRel, ZDBQueryType *query) {
	StringInfo request  = makeStringInfo();
	StringInfo postData = makeStringInfo();
	StringInfo response;
	Datum      count;

	appendStringInfo(postData, "{\"query\":%s}", convert_to_query_dsl(indexRel, query, false));
	appendStringInfo(request,
					 "%s%s/%s/_count?filter_path=count",
					 ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel),
					 ZDBIndexOptionsGetTypeName(indexRel));
	response = rest_call("GET", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));
	count    = DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data),
								   CStringGetTextDatum("count"));

	/* convert the count property to an int8 and return as uint64 */
	return DatumGetUInt64(DirectFunctionCall1(int8in, PointerGetDatum(TextDatumGetCString(count))));
}

ElasticsearchScrollContext *ElasticsearchOpenScroll(Relation indexRel, ZDBQueryType *userQuery, bool use_id, uint64 limit, List *highlights, char **extraFields, int nextraFields) {
	ElasticsearchScrollContext *context       = palloc0(sizeof(ElasticsearchScrollContext));
    char                       *queryDSL;
	StringInfo                 request        = makeStringInfo();
	StringInfo                 postData       = makeStringInfo();
	StringInfo                 docvalueFields = makeStringInfo();
	StringInfo                 response;
	char                       *sortJson;
	uint64                     queryLimit     = zdbquery_get_limit(userQuery);
	bool                       needScore;
	uint64                     offset;
	double                     min_score;
	void                       *jsonResponse, *hitsObject;
	char                       *error;
	int                        i;

	finish_inserts(false);

	limit     = queryLimit != 0 ? queryLimit : limit; /* prefer to use the limit specified in the query */
	needScore = zdbquery_get_wants_score(userQuery);
	offset    = zdbquery_get_offset(userQuery);
	sortJson  = zdbquery_get_sort_json(userQuery);
	min_score = zdbquery_get_min_score(userQuery);

    queryDSL = convert_to_query_dsl(indexRel, userQuery, limit > 0);

    /* we'll assume we want scoring if we have a limit w/o a sort, so that we get the top scoring docs when the limit is applied */
	needScore = needScore || (limit > 0 && sortJson == NULL);

	appendStringInfo(postData, "{\"track_scores\":%s,", needScore ? "true" : "false");
	if (min_score > 0) {
		appendStringInfo(postData, "\"min_score\":%f,", min_score);
	}
	if (sortJson != NULL) {
		appendStringInfo(postData, "\"sort\":%s,", sortJson);
	} else {
		appendStringInfo(postData, "\"sort\":[{\"%s\":\"%s\"}],", needScore ? "_score" : "_doc",
						 needScore ? "desc" : "asc");
	}
	appendStringInfo(postData, "\"query\":%s", queryDSL);

	if (highlights != NULL) {
		ListCell *lc;
		int      cnt = 0;

		appendStringInfo(postData, ",\"highlight\":{\"fields\":{");
		foreach (lc, highlights) {
			ZDBHighlightInfo *info = lfirst(lc);

			if (cnt > 0) appendStringInfoCharMacro(postData, ',');
			appendStringInfo(postData, "\"%s\":%s", info->name, info->json);
			cnt++;
		}
		appendStringInfo(postData, "}}");
	}

	appendStringInfoCharMacro(postData, '}');

	appendStringInfo(docvalueFields, "zdb_ctid");
	for (i = 0; i < nextraFields; i++) {
		appendStringInfo(docvalueFields, ",%s", extraFields[i]);
	}

	appendStringInfo(request,
					 "%s%s/%s/_search?_source=false&size=%lu&scroll=10m&filter_path=%s&stored_fields=%s&docvalue_fields=%s",
					 ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel),
					 ZDBIndexOptionsGetTypeName(indexRel),
					 limit == 0 ? MAX_DOCS_PER_REQUEST : Min(MAX_DOCS_PER_REQUEST, limit + offset),
					 ES_SEARCH_RESPONSE_FILTER,
					 highlights ? "type" : use_id ? "_id" : "_none_",
					 docvalueFields->data);

	response = rest_call("POST", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));

	/* create a memory context in which to allocate json data */
	context->jsonMemoryContext = AllocSetContextCreate(CurTransactionContext, "scroll", ALLOCSET_DEFAULT_MINSIZE,
													   4 * 1024 * 1024, ALLOCSET_DEFAULT_MAXSIZE);

	jsonResponse = parse_json_object(response, context->jsonMemoryContext);
	error        = get_json_object_object(jsonResponse, "error", true);
	if (error != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("%s", response->data)));

	hitsObject = get_json_object_object(jsonResponse, "hits", false);

	context->url              = ZDBIndexOptionsGetUrl(indexRel);
	context->compressionLevel = ZDBIndexOptionsGetCompressionLevel(indexRel);

	context->usingId       = use_id;
	context->scrollId      = get_json_object_string(jsonResponse, "_scroll_id", false);
	context->hasHighlights = highlights != NULL;
	context->cnt           = 0;
	context->currpos       = 0;
	context->total         =
			limit > 0 ? Min(limit, get_json_object_uint64(hitsObject, "total", false)) : get_json_object_uint64(
					hitsObject, "total", false);
	context->extraFields   = extraFields;
	context->nextraFields  = nextraFields;

	if (offset < context->total) {
		context->hits  = get_json_object_array(hitsObject, "hits", false);
		context->nhits = context->hits == NULL ? 0 : get_json_array_length(context->hits);

		/* fast-forward to our 'offset' -- using the ?from= ES request parameter doesn't work with scroll requests */
		if (offset > 0) {
			while (offset--) {
				if (!ElasticsearchGetNextItemPointer(context, NULL, NULL, NULL, NULL))
					break;
			}
		}
	} else {
		/*
		 * user specified an offset that's beyond the number of hits actually found, so just
		 * pretend we don't have any hits at all
		 */
		context->total = 0;
		context->nhits = 0;
	}

	pfree(queryDSL);
	freeStringInfo(request);
	freeStringInfo(postData);
	freeStringInfo(response);
	return context;
}

bool ElasticsearchGetNextItemPointer(ElasticsearchScrollContext *context, ItemPointer ctid, char **_id, float4 *score, zdb_json_object *highlights) {
	char *es_id = NULL;

start_over:

	if (context->cnt >= context->total) {
		if (context->cnt == 0) {
			/* we've run through all the rows AND exceeded the number of rows, so we're done */
			return false;
		}

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Attempt to read past total number of hits of %lu", context->total)));
	}

	if (context->currpos == context->nhits) {
		/* we exhausted the current set of hits, so go get more */
		StringInfo request  = makeStringInfo();
		StringInfo postData = makeStringInfo();
		StringInfo response;
		void       *jsonResponse, *hitsObject;
		char       *error;

		appendStringInfo(postData, "{\"scroll\":\"10m\",\"scroll_id\":\"%s\"}", context->scrollId);
		appendStringInfo(request, "%s_search/scroll?filter_path=%s", context->url, ES_SEARCH_RESPONSE_FILTER);
		response = rest_call("POST", request, postData, context->compressionLevel);

		/* make sure we don't leak the hits json from the previous request */
		if (context->hits != NULL)
			MemoryContextReset(context->jsonMemoryContext);

		jsonResponse = parse_json_object(response, context->jsonMemoryContext);
		error        = get_json_object_object(jsonResponse, "error", true);
		if (error != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("%s", response->data)));

		hitsObject = get_json_object_object(jsonResponse, "hits", false);

		context->scrollId = get_json_object_string(jsonResponse, "_scroll_id", false);
		context->currpos  = 0;
		context->hits     = get_json_object_array(hitsObject, "hits", false);
		context->nhits    = context->hits == NULL ? 0 : get_json_array_length(context->hits);

		freeStringInfo(request);
		freeStringInfo(response);
		freeStringInfo(postData);
	}

	if (context->hits == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("No results found when loading next scroll context")));

	context->hitEntry = get_json_array_element_object(context->hits, context->currpos, context->jsonMemoryContext);
	context->fields   = get_json_object_object(context->hitEntry, "fields", true);

	if (context->usingId) {
		es_id = (char *) get_json_object_string(context->hitEntry, "_id", false);
	} else if (ctid != NULL) {
		void   *zdb_ctid;
		uint64 ctidAs64bits;

		if (context->fields == NULL) {
		    /* there's no 'fields' block for this hit entry, which, by omission, indicates
		     * that this is the hit for the "zdb_aborted_xids" document, so we can just blindly
		     * skip to the next one
		     */
			context->cnt++;
			context->currpos++;
			if (context->cnt >= context->total)
				return false;

            goto start_over;
        }

		zdb_ctid     = get_json_object_array(context->fields, "zdb_ctid", false);
		ctidAs64bits = get_json_array_element_uint64(zdb_ctid, 0, context->jsonMemoryContext);

		/* set ctid out parameter */
		ItemPointerSet(ctid, (BlockNumber) (ctidAs64bits >> 32), (OffsetNumber) ctidAs64bits);
	}

	context->currpos++;
	context->cnt++;

	/* set our out parameters */
	if (_id != NULL) {
		*_id = es_id;
	}

	if (score != NULL) {
		*score = (float4) get_json_object_real(context->hitEntry, "_score");
	}

	if (highlights != NULL) {
		*highlights = context->hasHighlights ? get_json_object_object(context->hitEntry, "highlight", true) : NULL;
	}

	return true;
}

void ElasticsearchCloseScroll(ElasticsearchScrollContext *scrollContext) {
	MemoryContextDelete(scrollContext->jsonMemoryContext);
	pfree(scrollContext);
}

void ElasticsearchRemoveAbortedTransactions(Relation indexRel, List/*uint64*/ *xids) {
	if (list_length(xids) > 0) {
		StringInfo xidsArray = makeStringInfo();
		StringInfo request   = makeStringInfo();
		StringInfo postData  = makeStringInfo();
		StringInfo response;
		ListCell   *lc;

		foreach (lc, xids) {
			uint64 xid = *((uint64 *) lfirst(lc));

			if (xidsArray->len > 0) appendStringInfoCharMacro(xidsArray, ',');
			appendStringInfo(xidsArray, "%lu", xid);
		}

		appendStringInfo(postData, ""
								   "{"
								   "\"script\":{"
								   "\"source\":\"ctx._source.zdb_aborted_xids.removeAll(params.XIDS);\","
								   "\"params\":{\"XIDS\":[%s]},"
								   "\"lang\":\"painless\""
								   "}"
								   "}", xidsArray->data);

		appendStringInfo(request, "%s%s/%s/zdb_aborted_xids/_update?retry_on_conflict=128&refresh=true",
						 ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel),
						 ZDBIndexOptionsGetTypeName(indexRel));

		response = rest_call("POST", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));

		freeStringInfo(xidsArray);
		freeStringInfo(response);
		freeStringInfo(request);
		freeStringInfo(postData);
	}
}

void ElasticSearchForceMerge(Relation indexRel) {
	StringInfo request  = makeStringInfo();
	StringInfo response;

	appendStringInfo(request, "%s%s/_forcemerge?only_expunge_deletes=true&flush=false", ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("POST", request, NULL, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(response);

	resetStringInfo(request);
	appendStringInfo(request, "%s%s/_refresh", ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("POST", request, NULL, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(response);
	freeStringInfo(request);
}

char *ElasticsearchProfileQuery(Relation indexRel, ZDBQueryType *query) {
	StringInfo request  = makeStringInfo();
	StringInfo postData = makeStringInfo();
	StringInfo response;

	appendStringInfo(postData, "{\"profile\":true, \"query\":%s}", convert_to_query_dsl(indexRel, query, false));

	appendStringInfo(request, "%s%s/_search?size=0&filter_path=profile&pretty", ZDBIndexOptionsGetUrl(indexRel),
					 ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("POST", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(postData);
	freeStringInfo(request);

	return response->data;
}

uint64 ElasticsearchCount(Relation indexRel, ZDBQueryType *query) {
	StringInfo request  = makeStringInfo();
	StringInfo postData = makeStringInfo();
	StringInfo response;
	void       *json;
	uint64     count;

	validate_alias(indexRel);

	finish_inserts(false);

	appendStringInfo(postData, "{\"query\":%s}", convert_to_query_dsl(indexRel, query, true));

	appendStringInfo(request, "%s%s/_count?filter_path=count", ZDBIndexOptionsGetUrl(indexRel),
					 ZDBIndexOptionsGetAlias(indexRel));
	response = rest_call("POST", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));
	json     = parse_json_object(response, CurrentMemoryContext);
	count    = get_json_object_uint64(json, "count", false);

	pfree(json);
	freeStringInfo(response);
	freeStringInfo(postData);
	freeStringInfo(request);

	return count;
}

static char *makeAggRequest(Relation indexRel, ZDBQueryType *query, char *agg, bool arbitrary) {
	StringInfo request  = makeStringInfo();
	StringInfo postData = makeStringInfo();
	StringInfo response;

	validate_alias(indexRel);

	finish_inserts(false);

	appendStringInfoCharMacro(postData, '{');
	if (query != NULL)
		appendStringInfo(postData, "\"query\":%s,", convert_to_query_dsl(indexRel, query, true));

	if (arbitrary) {
		appendStringInfo(postData, "\"aggs\":%s", agg);
	} else {
		appendStringInfo(postData, "\"aggs\":{\"the_agg\":%s}", agg);
	}
	appendStringInfoCharMacro(postData, '}');

	appendStringInfo(request, "%s%s/_search?size=0", ZDBIndexOptionsGetUrl(indexRel),
					 ZDBIndexOptionsGetAlias(indexRel));
	response = rest_call("POST", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(postData);
	freeStringInfo(request);
	pfree(agg);

	return response->data;
}

char *ElasticsearchArbitraryAgg(Relation indexRel, ZDBQueryType *query, char *agg) {
	return makeAggRequest(indexRel, query, agg, true);
}

static char *makeTermsOrderClause(char *order) {
	char *clause;

	if (strcmp("count", order) == 0) {
		clause = psprintf(",\"order\":{\"_count\":\"desc\"}");
	} else if (strcmp("term", order) == 0) {
		clause = psprintf(",\"order\":{\"_term\":\"asc\"}");
	} else if (strcmp("reverse_count", order) == 0) {
		clause = psprintf(",\"order\":{\"_count\":\"asc\"}");
	} else if (strcmp("reverse_term", order) == 0) {
		clause = psprintf(",\"order\":{\"_term\":\"desc\"}");
	} else {
		clause = pstrdup("");    /* so that we can pfree it unconditionally */
	}

	return clause;
}

char *ElasticsearchTerms(Relation indexRel, char *field, ZDBQueryType *query, char *order, uint64 size) {
	char *orderClause = makeTermsOrderClause(order);
	char *response;

	if (size == 0)
		size = INT32_MAX;

	response = makeAggRequest(indexRel, query,
							  psprintf("{\"terms\":{\"field\":\"%s\",\"size\":%lu%s}}", field, size, orderClause),
							  false);

	pfree(orderClause);
	return response;
}

static StringInfo terms_agg_only_keys(Relation indexRel, char *field, ZDBQueryType *query, char *order, uint64 size) {
	char       *orderClause = makeTermsOrderClause(order);
	StringInfo request      = makeStringInfo();
	StringInfo postData     = makeStringInfo();
	StringInfo response;

	finish_inserts(false);

	if (size == 0)
		size = INT32_MAX;

	appendStringInfoCharMacro(postData, '{');
	if (query != NULL)
		appendStringInfo(postData, "\"query\":%s,", convert_to_query_dsl(indexRel, query, true));

	appendStringInfo(postData, "\"aggs\":{\"the_agg\":{\"terms\":{\"field\":\"%s\",\"size\":%lu%s}}}", field, size,
					 orderClause);
	appendStringInfoCharMacro(postData, '}');

	appendStringInfo(request, "%s%s/_search?size=0&filter_path=aggregations.the_agg.buckets.key",
					 ZDBIndexOptionsGetUrl(indexRel), ZDBIndexOptionsGetIndexName(indexRel));
	response = rest_call("POST", request, postData, ZDBIndexOptionsGetCompressionLevel(indexRel));

	freeStringInfo(postData);
	freeStringInfo(request);

	return response;
}

ArrayType *ElasticsearchTermsAsArray(Relation indexRel, char *field, ZDBQueryType *query, char *order, uint64 size) {
	StringInfo      response = terms_agg_only_keys(indexRel, field, query, order, size);
	ArrayBuildState *astate  = NULL;
	void            *json, *aggregations, *the_agg, *buckets;

	json         = parse_json_object(response, CurrentMemoryContext);
	aggregations = get_json_object_object(json, "aggregations", true);

	if (aggregations != NULL) {
		the_agg = get_json_object_object(aggregations, "the_agg", true);

		if (the_agg != NULL) {
			buckets = get_json_object_array(the_agg, "buckets", true);

			if (buckets != NULL) {
				int len = get_json_array_length(buckets);
				int i;

				for (i = 0; i < len; i++) {
					void       *obj  = get_json_array_element_object(buckets, i, CurrentMemoryContext);
					const char *term = get_json_object_string_force(obj, "key");

					astate = accumArrayResult(astate, CStringGetDatum(term), false, CSTRINGOID,
											  CurrentMemoryContext);
				}
			}
		}
	}

	pfree(json);
	if (astate != NULL)
		return DatumGetArrayTypeP(makeArrayResult(astate, CurrentMemoryContext));
	else
		return NULL;
}


char *ElasticsearchTermsTwoLevel(Relation indexRel, char *firstField, char *secondField, ZDBQueryType *query, char *order, uint64 size) {
	char *orderClause = makeTermsOrderClause(order);
	char *response;

	if (size == 0)
		size = INT32_MAX;

	response = makeAggRequest(indexRel, query, psprintf(
			"{"
			"\"terms\":{\"field\":\"%s\", \"size\":%lu%s},"
			"   \"aggregations\":{"
			"      \"sub_agg\":{"
			"         \"terms\":{\"field\":\"%s\",\"size\":%d}"
			"      }"
			"   }"
			"}",
			firstField, size, orderClause, secondField, INT32_MAX), false);

	pfree(orderClause);
	return response;
}


char *ElasticsearchAvg(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"avg\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchMin(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"min\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchMax(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"max\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchCardinality(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"cardinality\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchSum(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"sum\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchValueCount(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"value_count\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchPercentiles(Relation indexRel, char *field, ZDBQueryType *query, char *percents) {
	return makeAggRequest(indexRel, query, psprintf("{\"percentiles\":{\"field\":\"%s\"%s}}", field,
													strlen(percents) > 0 ? psprintf(",\"percents\":[%s]", percents)
																		 : ""), false);
}

char *ElasticsearchPercentileRanks(Relation indexRel, char *field, ZDBQueryType *query, char *values) {
	return makeAggRequest(indexRel, query, psprintf("{\"percentiles\":{\"field\":\"%s\"%s}}", field,
													strlen(values) > 0 ? psprintf(",\"values\":[%s]", values)
																	   : ""), false);
}

char *ElasticsearchStats(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"stats\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchExtendedStats(Relation indexRel, char *field, ZDBQueryType *query, int sigma) {
	return makeAggRequest(indexRel, query, psprintf("{\"extended_stats\":{\"field\":\"%s\"%s}}", field,
													sigma > 0 ? psprintf(",\"sigma\":%d", sigma) : ""), false);
}

char *ElasticsearchSignificantTerms(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"significant_terms\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchSignificantTermsTwoLevel(Relation indexRel, char *firstField, char *secondField, ZDBQueryType *query, uint64 size) {
	return makeAggRequest(indexRel, query, psprintf(
			"{"
			"\"terms\":{\"field\":\"%s\"%s},"
			"   \"aggregations\":{"
			"      \"sub_agg\":{"
			"         \"significant_terms\":{\"field\":\"%s\"}"
			"      }"
			"   }"
			"}",
			firstField, size > 0 ? psprintf(",\"size\":%lu", size) : "", secondField), false);
}

char *ElasticsearchRange(Relation indexRel, char *field, ZDBQueryType *query, char *ranges) {
	return makeAggRequest(indexRel, query, psprintf("{\"range\":{\"field\":\"%s\",\"ranges\":%s}}", field, ranges),
						  false);
}

char *ElasticsearchDateRange(Relation indexRel, char *field, ZDBQueryType *query, char *ranges) {
	return makeAggRequest(indexRel, query, psprintf("{\"date_range\":{\"field\":\"%s\",\"ranges\":%s}}", field, ranges),
						  0);
}

char *ElasticsearchHistogram(Relation indexRel, char *field, ZDBQueryType *query, float8 interval) {
	return makeAggRequest(indexRel, query,
						  psprintf("{\"histogram\":{\"field\":\"%s\",\"interval\":%f}}", field, interval), false);
}

char *ElasticsearchDateHistogram(Relation indexRel, char *field, ZDBQueryType *query, char *interval, char *format) {
	return makeAggRequest(indexRel, query,
						  psprintf("{\"date_histogram\":{\"field\":\"%s\",\"interval\":\"%s\",\"format\":\"%s\"}}",
								   field, interval, format), false);
}

char *ElasticsearchMissing(Relation indexRel, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"missing\":{\"field\":\"%s\"}}", field), false);
}

char *ElasticsearchFilters(Relation indexRel, char **labels, ZDBQueryType **filters, int nfilters) {
	StringInfo agg = makeStringInfo();
	int        i;

	appendStringInfo(agg, "{\"filters\":{\"filters\":{");
	for (i = 0; i < nfilters; i++) {
		if (i > 0) appendStringInfoCharMacro(agg, ',');
		appendStringInfo(agg, "\"%s\":%s", labels[i], convert_to_query_dsl(indexRel, filters[i], true));
	}
	appendStringInfo(agg, "}}}");

	return makeAggRequest(indexRel, NULL, agg->data, false);
}

char *ElasticsearchIPRange(Relation indexRel, char *field, ZDBQueryType *query, char *ranges) {
	return makeAggRequest(indexRel, query, psprintf("{\"ip_range\":{\"field\":\"%s\",\"ranges\":%s}}", field, ranges),
						  0);
}

char *ElasticsearchSignificantText(Relation indexRel, char *field, ZDBQueryType *query, int sample_size, bool filter_duplicate_text) {
	if (sample_size == 0)
		sample_size = INT32_MAX;
	return makeAggRequest(indexRel, query, psprintf(
			"{"
			"\"sampler\":{\"shard_size\":%d},"
			"   \"aggregations\":{"
			"      \"sub_agg\":{"
			"         \"significant_text\":{\"field\":\"%s\",\"filter_duplicate_text\":%s}"
			"      }"
			"   }"
			"}",
			sample_size, field, filter_duplicate_text ? "true" : "false"), false);
}

char *ElasticsearchAdjacencyMatrix(Relation indexRel, char **labels, ZDBQueryType **filters, int nfilters) {
	StringInfo agg = makeStringInfo();
	int        i;

	appendStringInfo(agg, "{\"adjacency_matrix\":{\"filters\":{");
	for (i = 0; i < nfilters; i++) {
		if (i > 0) appendStringInfoCharMacro(agg, ',');
		appendStringInfo(agg, "\"%s\":%s", labels[i], convert_to_query_dsl(indexRel, filters[i], true));
	}
	appendStringInfo(agg, "}}}");

	return makeAggRequest(indexRel, NULL, agg->data, false);
}

char *ElasticsearchMatrixStats(Relation indexRel, ZDBQueryType *query, char **fields, int nfields) {
	StringInfo agg = makeStringInfo();
	int        i;

	appendStringInfo(agg, "{\"matrix_stats\":{\"fields\":[");
	for (i = 0; i < nfields; i++) {
		if (i > 0) appendStringInfoCharMacro(agg, ',');
		appendStringInfo(agg, "\"%s\"", fields[i]);
	}
	appendStringInfo(agg, "]}}");

	return makeAggRequest(indexRel, query, agg->data, false);
}

char *ElasticsearchTopHits(Relation indexRel, ZDBQueryType *query, char **fields, int nfields, uint32 size) {
	StringInfo agg = makeStringInfo();
	int        i;

	if (size == 0)
		size = INT32_MAX;

	appendStringInfo(agg, "{\"top_hits\":{\"_source\":[");
	for (i = 0; i < nfields; i++) {
		if (i > 0) appendStringInfoCharMacro(agg, ',');
		appendStringInfo(agg, "\"%s\"", fields[i]);
	}
	appendStringInfoCharMacro(agg, ']');
	appendStringInfo(agg, ",\"size\":%u}}", size);

	return makeAggRequest(indexRel, query, agg->data, false);
}

char *ElasticsearchSampler(Relation indexRel, uint32 shard_size, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf(
			"{"
			"\"sampler\":{\"shard_size\":%d},"
			"   \"aggregations\":{"
			"      \"sub_agg\":{"
			"         \"terms\":{\"field\":\"zdb_ctid\",\"size\":%d}"
			"      }"
			"   }"
			"}",
			Max(1, shard_size / ZDBIndexOptionsGetNumberOfShards(indexRel)), INT32_MAX), false);
}

char *ElasticsearchDiversifiedSampler(Relation indexRel, uint32 shard_size, char *field, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf(
			"{"
			"\"diversified_sampler\":{\"shard_size\":%d,\"field\":\"%s\"},"
			"   \"aggregations\":{"
			"      \"sub_agg\":{"
			"         \"terms\":{\"field\":\"zdb_ctid\",\"size\":%d}"
			"      }"
			"   }"
			"}",
			Max(1, shard_size / ZDBIndexOptionsGetNumberOfShards(indexRel)), field, INT32_MAX), false);
}

char *ElasticsearchQuerySampler(Relation indexRel, ZDBQueryType *query) {
	return makeAggRequest(indexRel, query, psprintf("{\"terms\":{\"field\":\"zdb_ctid\",\"size\":%d}}", INT32_MAX),
						  false);
}

