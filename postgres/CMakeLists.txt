cmake_minimum_required(VERSION 3.3)
project(postgres)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/main/c/am/elasticsearch.c
    src/main/c/am/elasticsearch.h
    src/main/c/am/zdb_interface.c
    src/main/c/am/zdb_interface.h
    src/main/c/am/zdbagg.c
    src/main/c/am/zdbagg.h
    src/main/c/am/zdbam.c
    src/main/c/am/zdbam.h
    src/main/c/am/zdbops.c
    src/main/c/am/zdbops.h
    src/main/c/am/zdbseqscan.c
    src/main/c/am/zdbseqscan.h
    src/main/c/rest/rest.c
    src/main/c/rest/rest.h
    src/main/c/util/curl_support.c
    src/main/c/util/curl_support.h
    src/main/c/util/zdbutils.c
    src/main/c/util/zdbutils.h
    src/main/c/zombodb.c)

include_directories(src/main/c /usr/local/src/postgresql/src/include)

add_executable(postgres ${SOURCE_FILES})
