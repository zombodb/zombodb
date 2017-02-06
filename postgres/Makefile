##
# zombodb/Makefile
##
EXTENSION    = zombodb
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

MODULE_big   = $(EXTENSION)
OBJS = $(shell find src/main/c -type f -name "*.c" | sed s/\\.c/.o/g)

REGRESS = $(shell if [ "x${TEST}" != "x" ] ; then echo --use-existing ${TEST} ; else cat src/test/tests.list ; fi)
REGRESS_OPTS = --inputdir=src/test

PG_CPPFLAGS += -Isrc/main/c -I/usr/include
UNAME=$(shell uname -s)
WHOAMI=$(shell whoami)
ifeq ($(IS_DOCKER),true)
	# if building in docker, statically link libcurl
	SHLIB_LINK  += -Wl,-Bstatic -lcurl -Wl,-Bdynamic -lz -lrt
else
	SHLIB_LINK += -lcurl -lz -lrt
endif

PG_CONFIG    = pg_config

# default target
all:

src/test/sql/setup.sql:
	gunzip -c src/test/sql/setup.sql.gz > src/test/sql/setup.sql

src/test/sql/so_comments.sql:
	gunzip -c src/test/sql/so_comments.sql.gz > src/test/sql/so_comments.sql

src/test/sql/TUTORIAL.sql:
	gunzip -c src/test/sql/TUTORIAL.sql.gz > src/test/sql/TUTORIAL.sql

installcheck: src/test/sql/setup.sql src/test/sql/so_comments.sql src/test/sql/TUTORIAL.sql

installcheck-random: REGRESS = $(shell echo --use-existing && (cat src/test/tests.list | grep -v ^setup | grep -v ^so_comments | grep -v ^configure | grep -v ^create-indexes | grep -v ^TUTORIAL) | src/main/shell/mysort)
installcheck-random: installcheck

installcheck-concurrent: REGRESS = $(shell echo --use-existing && (cat src/test/tests.list | grep -v ^setup | grep -v ^so_comments | grep -v ^configure | grep -v ^create-indexes | grep -v ^TUTORIAL | grep -v deletes | grep -v updates) | src/main/shell/mysort)
installcheck-concurrent: installcheck


# add the SQL build target to the ALL build target
all: src/main/sql/$(EXTENSION)--$(EXTVERSION).sql

# target for building the SQL file
src/main/sql/$(EXTENSION)--$(EXTVERSION).sql: src/main/sql/$(EXTENSION).sql
	cp $< $@

# updated DATA with the SQL file for this version
DATA = $(wildcard src/main/sql/$(EXTENSION)--*--*.sql) src/main/sql/$(EXTENSION)--$(EXTVERSION).sql

ES_HOST ?= localhost

# add the SQL for this version to clean target
EXTRA_CLEAN += src/main/sql/$(EXTENSION)--$(EXTVERSION).sql src/test/sql/setup.sql src/test/sql/so_comments.sql src/test/sql/TUTORIAL.sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
