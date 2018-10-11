DROP FUNCTION zdb.visibility_clause(myXid bigint, myXmax bigint, myCid int, active_xids bigint[], index regclass, type text);
CREATE OR REPLACE FUNCTION zdb.visibility_clause(myXid bigint[], myXmax bigint, myCid int, active_xids bigint[], index regclass, type text) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
* ((Xmin == my-transaction &&				inserted by the current transaction
*	 Cmin < my-command &&					before this command, and
*	 (Xmax is null ||						the row has not been deleted, or
*	  (Xmax == my-transaction &&			it was deleted by the current transaction
*	   Cmax >= my-command)))				but not before this command,
* ||										or
*	(Xmin is committed &&					the row was inserted by a committed transaction, and
*		(Xmax is null ||					the row has not been deleted, or
*		 (Xmax == my-transaction &&			the row is being deleted by this transaction
*		  Cmax >= my-command) ||			but it's not deleted "yet", or
*		 (Xmax != my-transaction &&			the row was deleted by another transaction
*		  Xmax is not committed))))			that has not been committed
*/

/*
    (
        (xmin = "myXid"
             AND cmin < "myCid"
             AND (xmax = NULL OR (xmax = "myXid" AND cmax >= "myCid"))
        )
    OR
        (
            (NOT ({"terms":{"xmin":{"index":"$INDEX","path":"aborted_xids","type":"$TYPE","id":"aborted_xids"}}}) AND NOT xmin = [active_xids] AND NOT xmin >= "myXmax")
        AND (xmax = NULL
              OR (xmax = "myXid" AND cmax >= "myCid")
              OR (xmax != "myXid"
                    AND (({"terms":{"xmax":{"index":"$INDEX","path":"aborted_xids","type":"$TYPE","id":"aborted_xids"}}}) OR xmax = [active_xids] OR xmax >= "myXmax")
                 )
            )
        )
    )
*/

  SELECT dsl.must(
      dsl.noteq('_id:zdb_aborted_xids'),
      dsl.should(
          dsl.must(
              dsl.terms('zdb_xmin', VARIADIC myXid),
              dsl.range(field => 'zdb_cmin', lt => myCid),
              dsl.should(
                  dsl.field_missing('zdb_xmax'),
                  dsl.must(
                      dsl.terms('zdb_xmax', VARIADIC myXid),
                      dsl.range(field => 'zdb_cmax', gte => myCid)
                  )
              )
          ),
          dsl.must(
              dsl.must(
                  dsl.noteq(dsl.terms_lookup('zdb_xmin', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')),
                  dsl.noteq(dsl.terms('zdb_xmin', VARIADIC active_xids)),
                  dsl.noteq(dsl.range(field => 'zdb_xmin', gte => myXmax)),
                  dsl.should(
                      dsl.field_missing('zdb_xmax'),
                      dsl.must(
                          dsl.terms('zdb_xmax', VARIADIC myXid),
                          dsl.range(field => 'zdb_cmax', gte => myCid)
                      ),
                      dsl.must(
                          dsl.noteq(dsl.terms('zdb_xmax', VARIADIC myXid)),
                          dsl.should(
                              dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids'),
                              dsl.terms('zdb_xmax', VARIADIC active_xids),
                              dsl.range(field => 'zdb_xmax', gte => myXmax)
                          )
                      )
                  )
              )
          )
      )
  );
$$;

