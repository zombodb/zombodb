cmake_minimum_required(VERSION 3.5)
project(zombodb)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
        src/c/aggs/aggfuncs.c
        src/c/elasticsearch/elasticsearch.c
        src/c/elasticsearch/elasticsearch.h
        src/c/elasticsearch/mapping.c
        src/c/elasticsearch/mapping.h
        src/c/elasticsearch/querygen.c
        src/c/elasticsearch/querygen.h
        src/c/highlighting/highlighting.c
        src/c/highlighting/highlighting.h
        src/c/indexam/seqscan.c
        src/c/indexam/zdb_index_options.h
        src/c/indexam/zdbam.c
        src/c/indexam/zdbam.h
        src/c/indexam/zdbfuncs.c
        src/c/json/json.c
        src/c/json/json.h
        src/c/json/json_support.c
        src/c/json/json_support.h
        src/c/rest/curl_support.c
        src/c/rest/curl_support.h
        src/c/rest/rest.c
        src/c/rest/rest.h
        src/c/scoring/scoring.c
        src/c/scoring/scoring.h
        src/c/tablesamplers/common.c
        src/c/tablesamplers/common.h
        src/c/tablesamplers/diversified.c
        src/c/tablesamplers/diversified.h
        src/c/tablesamplers/query.c
        src/c/tablesamplers/query.h
        src/c/tablesamplers/sampler.c
        src/c/tablesamplers/sampler.h
        src/c/tablesamplers/tablesamplers.c
        src/c/type/zdbquerytype.c
        src/c/type/zdbquerytype.h
        src/c/utils/utils.c
        src/c/utils/utils.h
        src/c/zombodb.c
        src/c/indexam/llapi.c)

include_directories(src/c /usr/local/src/pg10/src/include /usr/local/src/pg10/src/backend)

add_library(zombodb ${SOURCE_FILES})


