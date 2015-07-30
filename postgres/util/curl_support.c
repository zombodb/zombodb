#include "postgres.h"
#include "utils/memutils.h"

#include "curl_support.h"

/**
 * In all the cases below, make sure that the memory libcurl allocates
 * is attached to the TopTransactionContext, rather than the CurTransactionContext.
 *
 * This is to ensure that the various curl_multi_xxx() calls (which are typically called
 * during index creation and INSERT/UPDATE/DELETE) survive while Postgres continues
 * on to the next tuple.
 */


void *zdb_curl_calloc_wrapper(size_t count, size_t size) {
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	void *mem = palloc0(count*size+1);
	MemoryContextSwitchTo(oldContext);
	return mem;
}

void *zdb_curl_palloc_wrapper(Size size) {
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	void *mem = palloc(size);
	MemoryContextSwitchTo(oldContext);
	return mem;
}

void *zdb_curl_repalloc_wrapper(void *pointer, Size size) {
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	void *mem = repalloc(pointer, size);
	MemoryContextSwitchTo(oldContext);
	return mem;
}

char *zdb_curl_pstrdup_wrapper(char const *str) {
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *mem = pstrdup(str);
	MemoryContextSwitchTo(oldContext);
	return mem;
}

void zdb_curl_pfree_wrapper(void *pointer) {
	/*
	 * libcurl sometimes calls free(NULL) but Postgres' pfree() will Assert
	 * in that condition, so guard against it
	 */
	if (pointer) pfree(pointer);
}

