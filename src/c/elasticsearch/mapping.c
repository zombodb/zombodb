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

#include "mapping.h"

#include "executor/spi.h"
#include "utils/typcache.h"

static bool lookup_field_mapping(Oid tableRelId, char *fieldname, StringInfo mapping, MemoryContext memcxt) {
	static Oid   types[2]  = {REGCLASSOID, TEXTOID};
	Datum datums[2] = {ObjectIdGetDatum(tableRelId), CStringGetTextDatum(fieldname)};
	char  nulls[2]  = {0, 0};
	int   res;
	bool  rc        = false;

	SPI_connect();
	if ((res = SPI_execute_with_args(
			"select (to_json(field_name) || ':' || definition) from zdb.mappings where table_name = $1::regclass and field_name = $2;",
			2,
			types,
			datums,
			nulls,
			true,
			1)) != SPI_OK_SELECT)
		elog(ERROR, "Problem looking up analysis thing, result=%d", res);

	if (SPI_processed > 1) {
		elog(ERROR, "Too many mappings found");
	} else if (SPI_processed == 1) {
		char *json = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		MemoryContext  oldcontext;

		oldcontext = MemoryContextSwitchTo(memcxt);
		appendStringInfo(mapping, ", %s", json);
		MemoryContextSwitchTo(oldcontext);

		rc = true;
	}

	SPI_finish();

	return rc;
}

static bool lookup_type_mapping(Oid typeOid, int32 typmod, char *fieldname, StringInfo mapping, MemoryContext memcxt) {
	static Oid   types[1]  = {REGTYPEOID};
	Datum datums[1] = {ObjectIdGetDatum(typeOid)};
	char  nulls[1]  = {0};
	int   res;
	bool  rc        = false;

	SPI_connect();
	if ((res = SPI_execute_with_args("select definition, funcid from zdb.type_mappings where type_name = $1::regtype;",
									 1,
									 types,
									 datums,
									 nulls,
									 true,
									 1)) != SPI_OK_SELECT)
		elog(ERROR, "Problem looking up type mapping, result=%d", res);

	if (SPI_processed > 1) {
		elog(ERROR, "Too many type mappings found");
	} else if (SPI_processed == 1) {
		MemoryContext oldcontext;
		Jsonb *json = NULL;
		Datum val;
		bool isnull;

		val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull) {
			/* it's a json definition */
			json = DatumGetJsonbP(val);
		} else {
			val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
			if (!isnull) {
				/* it's a function conversion */
				Oid funcid = DatumGetObjectId(val);

				json = DatumGetJsonbP(OidFunctionCall2(funcid, ObjectIdGetDatum(typeOid), Int32GetDatum(typmod)));
			} else {
				elog(ERROR, "zdb.type_mappings row for type oid %u is invalid", typeOid);
			}
		}

		if (json == NULL)
			elog(ERROR, "couldn't get jsonb value for type mapping");

		oldcontext = MemoryContextSwitchTo(memcxt);
		appendStringInfo(mapping, ", \"%s\":", fieldname);
		JsonbToCString(mapping, &json->root, VARSIZE(json));
		MemoryContextSwitchTo(oldcontext);

		rc = true;
	}

	SPI_finish();

	return rc;
}

static bool type_is_domain(char *type_name, Oid *base_type) {
	static Oid   types[1]  = {TEXTOID};
	Datum values[1] = {CStringGetTextDatum(type_name)};
	char  nulls[1]  = {0};
	int   res;
	bool  rc;

	SPI_connect();
	if ((res = SPI_execute_with_args("SELECT typtype = 'd', typbasetype FROM pg_type WHERE oid = $1::regtype;",
									 1,
									 types,
									 values,
									 nulls,
									 true,
									 1)) != SPI_OK_SELECT)
		elog(ERROR, "Problem determining if '%s' is a domain, result=%d", type_name, res);

	if (SPI_processed == 0) {
		rc = false;
	} else {
		bool  isnull;
		Datum d;

		d  = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		rc = isnull || DatumGetBool(d);

		d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
		*base_type = isnull ? InvalidOid : DatumGetObjectId(d);
	}

	SPI_finish();

	return rc;
}


char *lookup_analysis_thing(MemoryContext cxt, char *thing) {
	char *definition = "";
	int  res;

	SPI_connect();

	if ((res = SPI_execute(psprintf("select (to_json(name) || ':' || definition) from zdb.%s;",
									TextDatumGetCString(DirectFunctionCall1(quote_ident, CStringGetTextDatum(thing)))),
						   true,
						   0)) != SPI_OK_SELECT)
		elog(ERROR, "Problem looking up analysis thing '%s', result=%d", thing, res);

	if (SPI_processed > 0) {
		StringInfo json = makeStringInfo();
		uint64     i;

		for (i = 0; i < SPI_processed; i++) {
			if (i > 0) appendStringInfoCharMacro(json, ',');
			appendStringInfo(json, "%s", SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
		}
		definition = (char *) MemoryContextAllocZero(cxt, (Size) json->len + 1);
		memcpy(definition, json->data, json->len);
	}

	SPI_finish();

	return definition;
}

static List *lookup_es_only_fields(MemoryContext cxt, Oid tableOid) {
	static Oid   types[1]  = {REGCLASSOID};
	Datum values[1] = {tableOid};
	char  nulls[1]  = {0};
	List  *response = NULL;
	int   res;

	SPI_connect();
	if ((res = SPI_execute_with_args(
			"select (to_json(field_name) || ':' || definition) from zdb.mappings WHERE table_name = $1::regclass and es_only = true;",
			1,
			types,
			values,
			nulls,
			true,
			0)) != SPI_OK_SELECT)
		elog(ERROR, "Problem looking up es-only fields, result=%d", res);

	if (SPI_processed > 0) {
		MemoryContext oldContext = MemoryContextSwitchTo(cxt);
		uint64        i;

		for (i = 0; i < SPI_processed; i++) {
			response = lappend(response, SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
		}

		MemoryContextSwitchTo(oldContext);
	}

	SPI_finish();

	return response;
}


StringInfo generate_mapping(Relation heapRel, TupleDesc tupdesc) {
	StringInfo mapping = makeStringInfo();
	int        i;
	ListCell   *lc;

	appendStringInfo(mapping, "\"zdb_all\": { \"type\":\"text\", \"analyzer\":\"zdb_all_analyzer\" }");
	appendStringInfo(mapping, ",\"zdb_ctid\": { \"type\":\"long\" }");
	appendStringInfo(mapping, ",\"zdb_cmin\": { \"type\":\"integer\" }");
	appendStringInfo(mapping, ",\"zdb_cmax\": { \"type\":\"integer\" }");
	appendStringInfo(mapping, ",\"zdb_xmin\": { \"type\":\"long\" }");
	appendStringInfo(mapping, ",\"zdb_xmax\": { \"type\":\"long\" }");
	appendStringInfo(mapping, ",\"zdb_aborted_xids\": { \"type\":\"long\" }");

	foreach (lc, lookup_es_only_fields(CurrentMemoryContext, RelationGetRelid(heapRel))) {
		char *json = lfirst(lc);
		appendStringInfo(mapping, ",%s", json);
	}

	for (i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		Oid               typeOid = get_base_type_oid(attr->atttypid);
		TypeCacheEntry    *cacheEntry;
		char *typename;

		/* skip dropped attributes */
		if (attr->attisdropped)
			continue;

		/* apply field-specific mapping, if we have it in the database */
		if (lookup_field_mapping(RelationGetRelid(heapRel), NameStr(attr->attname), mapping, CurrentMemoryContext))
			continue;

		/* apply type-specific mapping, if we have it in the database */
		if (lookup_type_mapping(typeOid, attr->atttypmod, NameStr(attr->attname), mapping, CurrentMemoryContext))
			continue;


		/* figure out what to do based on heuristics regarding DOMAINs that map to analyzer names */
		appendStringInfo(mapping, ", \"%s\": {", NameStr(attr->attname));
		cacheEntry = lookup_type_cache(attr->atttypid, 0);

		typename = DatumGetCString(DirectFunctionCall1(regtypeout, Int32GetDatum(attr->atttypid)));
		if (cacheEntry->typtype == 'd') {
			/*
			 * it's a domain type, so we set it to a type of text or keyword
			 * depending on if its base type is ::text or ::varchar
			 *
			 * we also assign an analyzer (for text) or normalizer (for keyword) that is the same name as the domain
			 * type itself
			 */
			Oid  base_type = InvalidOid;

			type_is_domain(typename, &base_type);

			/* strip schema name, if we have one */
			if (strchr(typename, '.') != 0) {
				typename = strchr(typename, '.') + 1;
			}

			switch (base_type) {
				case VARCHAROID:
					if (strcmp("keyword", typename) == 0) {
						/* if the typename is 'keyword', then we don't need to set the normalizer */
						appendStringInfo(mapping, "\"type\":\"keyword\","
												  "\"ignore_above\": 10922");
					} else {
						/* otherwise, the normalizer is set to the typename */
						appendStringInfo(mapping, "\"type\":\"keyword\","
												  "\"ignore_above\": 10922,"
												  "\"normalizer\":\"%s\"", typename);
					}
					break;
				case TEXTOID:
					appendStringInfo(mapping, "\"type\":\"text\","
											  "\"analyzer\":\"%s\"", typename);
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
									errmsg("Unsupported base domain type for %s: %u", typename, base_type)));
			}
		} else {
			/* it's a type that we don't have built-in knowledge on how to map, so treat it as a 'keyword' */
			elog(NOTICE, "[zombodb] unrecognized data type '%s', mapping to 'keyword'", typename);
			appendStringInfo(mapping, "\"type\":\"keyword\","
									  "\"ignore_above\": 10922,"
									  "\"normalizer\":\"lowercase\"");
		}

		appendStringInfo(mapping, "}");
	}

	return mapping;
}
