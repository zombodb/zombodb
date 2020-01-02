#
#  Copyright 2018-2019 ZomboDB, LLC
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

#
# figure out the build target mode based on the TARGET envvar
#
ifeq ($(TARGET),release)
	BUILD_TARGET = release
	CARGO_TARGET = --release
	OPT_LEVEL = 2
else
	BUILD_TARGET = debug
	CARGO_TARGET =
	OPT_LEVEL = 0
endif

#
# how is our shard lib named
#
ifeq ($(UNAME),Linux)
	LINK_ARGS =
else
	LINK_ARGS = -framework CoreFoundation -framework IOKit -framework Security
endif

#
# extension definition
#

EXTENSION = zombodb
EXTVERSION = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")
MODULE_big = $(EXTENSION)
DATA = $(wildcard src/sql/$(EXTENSION)--*--*.sql) src/sql/$(EXTENSION)--$(EXTVERSION).sql
PGFILEDESC = "ZomboDB"
UNAME = $(shell uname)
RPATH = $(shell pg_config --sharedir)/extension

#
# object files
#

PG_CPPFLAGS += -Isrc/c/ -O$(OPT_LEVEL)
SHLIB_LINK += -lcurl -lz $(LINK_ARGS)
OBJS = $(shell find src/c -type f -name "*.c" | sed s/\\.c/.o/g) src/rust/target/$(BUILD_TARGET)/libzdb_helper.a

#
# make targets
#

all: src/sql/$(EXTENSION)--$(EXTVERSION).sql src/rust/target/$(BUILD_TARGET)/libzdb_helper.a

src/rust/target/$(BUILD_TARGET)/libzdb_helper.a: src/rust/.cargo/config $(shell find src/rust/ -type f -name '*.toml') $(shell find src/rust/ -type f -name '*.rs' | grep -v target/) $(shell find ../pg-rs-bridge/ -type f -name '*.rs' | grep -v target/)
	cd src/rust && cargo build $(CARGO_TARGET)

src/sql/$(EXTENSION)--$(EXTVERSION).sql: src/sql/order.list
	cat `cat $<` > $@

#
# regression testing vars
#

REGRESS = $(shell if [ "x${TEST}" != "x" ] ; then echo --use-existing ${TEST} ; else (echo --use-existing && ls src/test/sql/test-*.sql src/test/sql/issue-*.sql | cut -f4 -d/ | cut -f1 -d . | sort) ; fi)
REGRESS_OPTS = --inputdir=src/test

src/test/sql/load-data.sql: src/test/sql/load-data.tmpl
	cat src/test/sql/load-data.tmpl | sed -e "s:@PWD:`pwd`:g" > src/test/sql/load-data.sql
	cat src/test/sql/load-data.tmpl | sed -e "s:@PWD:`pwd`:g" > src/test/expected/load-data.out

installcheck-setup: src/test/sql/load-data.sql
	(dropdb contrib_regression; exit 0)
	createdb contrib_regression
	TEST="setup create-tables load-data index-data" make installcheck

checkboth:
	./checkboth.sh	

#
# "make clean"
#

EXTRA_CLEAN += src/sql/$(EXTENSION)--$(EXTVERSION).sql src/rust/target

#
# build targets
#

flint:
	src/flint/runflint.sh > src/flint/flint.out

release:
	cd build && ./build.sh
	open target/artifacts

#
# PGXS usage
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
