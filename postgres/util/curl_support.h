#include "postgres.h"

void *zdb_curl_calloc_wrapper(size_t count, size_t size);
void *zdb_curl_palloc_wrapper(Size size);
void *zdb_curl_repalloc_wrapper(void *pointer, Size size);
char *zdb_curl_pstrdup_wrapper(char const *str);
void zdb_curl_pfree_wrapper(void *pointer);
