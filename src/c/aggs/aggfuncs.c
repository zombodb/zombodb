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

#include "elasticsearch/elasticsearch.h"

PG_FUNCTION_INFO_V1(zdb_count);
PG_FUNCTION_INFO_V1(zdb_arbitrary_agg);
PG_FUNCTION_INFO_V1(zdb_internal_terms);
PG_FUNCTION_INFO_V1(zdb_internal_terms_array);
PG_FUNCTION_INFO_V1(zdb_internal_terms_two_level);
PG_FUNCTION_INFO_V1(zdb_internal_avg);
PG_FUNCTION_INFO_V1(zdb_internal_min);
PG_FUNCTION_INFO_V1(zdb_internal_max);
PG_FUNCTION_INFO_V1(zdb_internal_cardinality);
PG_FUNCTION_INFO_V1(zdb_internal_sum);
PG_FUNCTION_INFO_V1(zdb_internal_value_count);
PG_FUNCTION_INFO_V1(zdb_internal_percentiles);
PG_FUNCTION_INFO_V1(zdb_internal_percentile_ranks);
PG_FUNCTION_INFO_V1(zdb_internal_stats);
PG_FUNCTION_INFO_V1(zdb_internal_extended_stats);
PG_FUNCTION_INFO_V1(zdb_internal_significant_terms);
PG_FUNCTION_INFO_V1(zdb_internal_significant_terms_two_level);
PG_FUNCTION_INFO_V1(zdb_internal_range);
PG_FUNCTION_INFO_V1(zdb_internal_date_range);
PG_FUNCTION_INFO_V1(zdb_internal_histogram);
PG_FUNCTION_INFO_V1(zdb_internal_date_histogram);
PG_FUNCTION_INFO_V1(zdb_internal_missing);
PG_FUNCTION_INFO_V1(zdb_internal_filters);
PG_FUNCTION_INFO_V1(zdb_internal_ip_range);
PG_FUNCTION_INFO_V1(zdb_internal_significant_text);
PG_FUNCTION_INFO_V1(zdb_internal_adjacency_matrix);
PG_FUNCTION_INFO_V1(zdb_internal_matrix_stats);
PG_FUNCTION_INFO_V1(zdb_internal_top_hits);

Datum zdb_count(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
	Relation     indexRel;
	uint64       count;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	count    = ElasticsearchCount(indexRel, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_INT64(count);
}

Datum zdb_arbitrary_agg(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
	char         *agg        = GET_STR(PG_GETARG_TEXT_P(2));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchArbitraryAgg(indexRel, query, agg);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_terms(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *order      = GET_STR(PG_GETARG_TEXT_P(3));
	uint64       limit       = PG_GETARG_INT64(4);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchTerms(indexRel, field, query, order, limit);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_terms_array(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *order      = GET_STR(PG_GETARG_TEXT_P(3));
	uint64       limit       = PG_GETARG_INT64(4);
	Relation     indexRel;
	ArrayType    *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchTermsAsArray(indexRel, field, query, order, limit);
	relation_close(indexRel, AccessShareLock);

	if (response != NULL)
		PG_RETURN_ARRAYTYPE_P(response);
	else
		PG_RETURN_NULL();
}

Datum zdb_internal_terms_two_level(PG_FUNCTION_ARGS) {
	Oid          indexRelOid  = PG_GETARG_OID(0);
	char         *firstField  = GET_STR(PG_GETARG_TEXT_P(1));
	char         *secondField = GET_STR(PG_GETARG_TEXT_P(2));
	ZDBQueryType *query       = (ZDBQueryType *) PG_GETARG_VARLENA_P(3);
	char         *order       = GET_STR(PG_GETARG_TEXT_P(4));
	uint64       limit        = PG_GETARG_INT64(5);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchTermsTwoLevel(indexRel, firstField, secondField, query, order, limit);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_avg(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchAvg(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_min(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchMin(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_max(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchMax(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_cardinality(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchCardinality(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_sum(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchSum(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_value_count(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchValueCount(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_percentiles(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *percents   = GET_STR(PG_GETARG_TEXT_P(3));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchPercentiles(indexRel, field, query, percents);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_percentile_ranks(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *values     = GET_STR(PG_GETARG_TEXT_P(3));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchPercentileRanks(indexRel, field, query, values);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_stats(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchStats(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_extended_stats(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	int          sigma       = PG_GETARG_INT32(3);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchExtendedStats(indexRel, field, query, sigma);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_significant_terms(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchSignificantTerms(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_significant_terms_two_level(PG_FUNCTION_ARGS) {
	Oid          indexRelOid  = PG_GETARG_OID(0);
	char         *firstField  = GET_STR(PG_GETARG_TEXT_P(1));
	char         *secondField = GET_STR(PG_GETARG_TEXT_P(2));
	ZDBQueryType *query       = (ZDBQueryType *) PG_GETARG_VARLENA_P(3);
	uint64       size         = PG_GETARG_INT64(4);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchSignificantTermsTwoLevel(indexRel, firstField, secondField, query, size);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_range(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *ranges     = GET_STR(PG_GETARG_TEXT_P(3));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchRange(indexRel, field, query, ranges);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_date_range(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *ranges     = GET_STR(PG_GETARG_TEXT_P(3));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchDateRange(indexRel, field, query, ranges);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_histogram(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	float8       interval    = PG_GETARG_FLOAT8(3);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchHistogram(indexRel, field, query, interval);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_date_histogram(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *interval   = GET_STR(PG_GETARG_TEXT_P(3));
	char         *format     = GET_STR(PG_GETARG_TEXT_P(4));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchDateHistogram(indexRel, field, query, interval, format);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_missing(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchMissing(indexRel, field, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_filters(PG_FUNCTION_ARGS) {
	Oid          indexRelOid   = PG_GETARG_OID(0);
	ArrayType    *labelsArray  = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType    *filtersArray = PG_GETARG_ARRAYTYPE_P(2);
	int          nlabels;
	int          nfilters;
	char         **labels      = array_to_strings(labelsArray, &nlabels);
	ZDBQueryType **filters     = array_to_zdbqueries(filtersArray, &nfilters);
	Relation     indexRel;
	char         *response;

	if (nlabels != nfilters) {
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
						errmsg("Number of labels and filters do not match")));
	} else if (array_contains_nulls(labelsArray)) {
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
						errmsg("labels array cannot contain NULL values")));
	} else if (array_contains_nulls(filtersArray)) {
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
						errmsg("filters array cannot contain NULL values")));
	}

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchFilters(indexRel, labels, filters, nfilters);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_ip_range(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	char         *field      = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	char         *ranges     = GET_STR(PG_GETARG_TEXT_P(3));
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchIPRange(indexRel, field, query, ranges);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_significant_text(PG_FUNCTION_ARGS) {
	Oid          indexRelOid           = PG_GETARG_OID(0);
	char         *field                = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType *query                = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	int          sample_size           = PG_GETARG_INT32(3);
	bool         filter_duplicate_text = PG_GETARG_BOOL(4);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchSignificantText(indexRel, field, query, sample_size, filter_duplicate_text);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_adjacency_matrix(PG_FUNCTION_ARGS) {
	Oid          indexRelOid   = PG_GETARG_OID(0);
	ArrayType    *labelsArray  = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType    *filtersArray = PG_GETARG_ARRAYTYPE_P(2);
	int          nlabels;
	int          nfilters;
	char         **labels      = array_to_strings(labelsArray, &nlabels);
	ZDBQueryType **filters     = array_to_zdbqueries(filtersArray, &nfilters);
	Relation     indexRel;
	char         *response;

	if (nlabels != nfilters) {
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
						errmsg("Number of labels and filters do not match")));
	} else if (array_contains_nulls(labelsArray)) {
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
						errmsg("labels array cannot contain NULL values")));
	} else if (array_contains_nulls(filtersArray)) {
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
						errmsg("filters array cannot contain NULL values")));
	}

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchAdjacencyMatrix(indexRel, labels, filters, nfilters);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_matrix_stats(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	int          nfields;
	char         **fields    = array_to_strings(PG_GETARG_ARRAYTYPE_P(1), &nfields);
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchMatrixStats(indexRel, query, fields, nfields);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}

Datum zdb_internal_top_hits(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	int          nfields;
	char         **fields    = array_to_strings(PG_GETARG_ARRAYTYPE_P(1), &nfields);
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	uint32       size        = PG_GETARG_UINT32(3);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchTopHits(indexRel, query, fields, nfields, size);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(response));
}


