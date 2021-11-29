##
# zombodb/Makefile
##
tests/expand-same.sql:
	./build-same.sh > tests/expand-same.sql

pgtap: tests/expand-same.sql
	if [ "x" = "x${TEST}" ] ; then psql -qAt template1 -f sql/teardown.sql ; fi
	if [ "x" = "x${TEST}" ] ; then psql -qAt template1 -f sql/setup.sql ; fi
	if [ "x" = "x${TEST}" ] ; then pg_prove -d zdbtaptests tests/*.sql ; fi
