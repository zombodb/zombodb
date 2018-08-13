#
#  Copyright 2018 ZomboDB, LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#! /bin/bash

flint \
-summary \
-efunc 545,hash_search \
-emacro 826,\*Datum\* \
-emacro \*,elog \
-esym 40,va_data \
-esym 762,no_such_variable \
-emacro \*,memcpy \
-emacro \*,makeNode \
-emacro \*,offsetof \
-emacro \*,PG\*_TRY \
-emacro 701,ItemPointerGetBlockNumber \
-emacro 571,BufferGetPage \
-emacro 666,BufferGetPage \
-esym 641,relopt_kind \
-esym \*,fcinfo \
-emacro \*,ereport \
-emacro \*,rt_fetch \
-emacro \*,copyObject \
-esym \*,__builtin_object_size,__builtin___memset_chk \
-esym 534,MemoryContextSwitchTo \
-esym 534,SPI_connect \
-esym 534,SPI_finish \
-ecall 747,Elasticsearch\* \
-ecall 747,hash_create \
-ecall 747,AllocSetContextCreate \
-ecall 571,MemoryContextAllocZero \
-ecall 747,SPI_execute \
-ecall 747,fillRelOptions \
-ecall 747,palloc0 \
-ecall 747,palloc \
-ecall 737,palloc \
-ecall 571,json_parse_ex \
-ecall 571,compressBound \
-ecall 571,compress2 \
-ecall 747,curl_global_init \
-ecall 835,curl_global_init \
-e78 -e19 -e10 -e505 -e506 -e526 -e537 -e628 -e686 -e714 -e717 -e740 -e765 -e768 -e769 -e788 -e793 -e801 -e818 -e820 -e826 -e830 -e835 \
-i /usr/local/src/pg10/src/include \
-i src/flint \
-i src/c \
-wlib 0 \
+libclass foreign \
+libdir json \
+libdir /usr/local/src/pg10/src/include \
src/flint/co-gcc.lnt $(find src/c/ -name "*.[c]")
