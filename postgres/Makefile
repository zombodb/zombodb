##
# zombodb/Makefile
##
EXTENSION    = zombodb
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

MODULE_big   = $(EXTENSION)
OBJS = \
    zombodb.o                     \
    rest/rest.o                   \
    util/zdbutils.o               \
    am/zdbam.o                    \
    am/zdbops.o                   \
    am/zdbagg.o                   \
    am/zdb_interface.o            \
    am/elasticsearch.o

REGRESS = $(shell if [ "x${TEST}" != "x" ] ; then echo --use-existing ${TEST} ; else cat test/tests.list ; fi)
REGRESS_OPTS = --inputdir=test

FLEXFLAGS    = -8 -Pfti
SHLIB_LINK  += -L/usr/local/opt/curl/lib -lcurl
PG_CONFIG    = pg_config

test/sql/setup.sql:
	gunzip -c test/sql/setup.sql.gz > test/sql/setup.sql

installcheck: test/sql/setup.sql

installcheck-random: REGRESS = $(shell echo --use-existing && (cat test/tests.list | grep -v ^setup | grep -v ^configure | grep -v ^create-indexes) | test/mysort)
installcheck-random: installcheck

# add the SQL build target to the ALL build target
all: sql/$(EXTENSION)--$(EXTVERSION).sql

# target for building the SQL file
sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

# updated DATA with the SQL file for this version
DATA = $(wildcard sql/$(EXTENSION)--*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql

ES_HOST ?= localhost

# add the SQL for this version to clean target
EXTRA_CLEAN += sql/$(EXTENSION)--$(EXTVERSION).sql test/sql/setup.sql

CFLAGS := -I/usr/local/include $(CFLAGS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
LDFLAGS := $(addprefix -L/usr/local/lib/ -L/usr/local/opt/curl/lib , $(LDFLAGS))


override CFLAGS += -I/usr/local/include

# distribution target
dist:
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ -o $(EXTENSION)-$(EXTVERSION).zip HEAD
# add distribution file to the clean target
EXTRA_CLEAN += $(EXTENSION)-$(EXTVERSION).zip
