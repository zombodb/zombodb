#
# This copies "expand.sql" and builds a new version against the view called "consolidated_record_view_same"
# to assert that all the queries in expand.sql return the same results against a view (with a set of index links)
# where the key columns have the same name -- in this case "id"
#
# This is a re-create for issue #185 (https://github.com/zombodb/zombodb/issues/185) that hopefully will automatically
# pickup potential bugs that are added as test cases in expand.sql
#
# This script is called automatically by "make installcheck" and "make pgtap" -- there is no need to call it manually
#
#! /bin/sh

SOURCE_FILE=src/test/zdbtaptests/tests/expand.sql

echo "BEGIN;"
grep "SELECT plan" $SOURCE_FILE

IFS="
"
x=1 
for f in `grep "PREPARE zdb_result" $SOURCE_FILE` ; do 
	echo "DEALLOCATE ALL;"
	echo $f | sed 's/zdb_result/expected_result/'
	echo $f | sed 's/pk_data/id/' | sed 's/consolidated_record_view/consolidated_record_view_same/' | sed 's/zdb_result/zdb_result     /'
	echo "SELECT set_eq('expected_result', 'zdb_result', '$x');"
	x=$((x+1))
	echo "--***"
	echo
done

echo "SELECT * FROM finish();"
echo "ROLLBACK;"

