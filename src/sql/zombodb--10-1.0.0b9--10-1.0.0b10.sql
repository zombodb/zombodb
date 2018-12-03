
DROP FUNCTION zdb.visibility_clause;
DROP FUNCTION zdb.vac_by_xmin;
DROP FUNCTION zdb.vac_by_xmax;
DROP FUNCTION zdb.vac_aborted_xmax;

DROP FUNCTION dsl.should;
DROP FUNCTION dsl.must;
DROP FUNCTION dsl.must_not;
DROP FUNCTION dsl.filter;



--
-- query dsl changes to should/must/must_not/filter
--

CREATE TYPE dsl.esqdsl_must AS (queries zdbquery[]);
CREATE TYPE dsl.esqdsl_must_not AS (queries zdbquery[]);
CREATE TYPE dsl.esqdsl_should AS (queries zdbquery[]);
CREATE TYPE dsl.esqdsl_filter AS (queries zdbquery[]);

CREATE OR REPLACE FUNCTION dsl.bool(must dsl.esqdsl_must DEFAULT NULL, must_not dsl.esqdsl_must_not DEFAULT NULL, should dsl.esqdsl_should DEFAULT NULL, filter dsl.esqdsl_filter DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('bool', json_build_object('must', must.queries, 'must_not', must_not.queries, 'should', should.queries, 'filter', filter.queries)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.should(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_should PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(queries)::dsl.esqdsl_should;
$$;
CREATE OR REPLACE FUNCTION dsl.must(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_must PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(queries)::dsl.esqdsl_must;
$$;
CREATE OR REPLACE FUNCTION dsl.must_not(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_must_not PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(queries)::dsl.esqdsl_must_not;
$$;
CREATE OR REPLACE FUNCTION dsl.filter(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_filter PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(queries)::dsl.esqdsl_filter;
$$;
CREATE OR REPLACE FUNCTION dsl.noteq(query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(must_not=>dsl.must_not(query));
$$;
CREATE OR REPLACE FUNCTION dsl.not(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(must_not=>dsl.must_not(VARIADIC queries));
$$;
CREATE OR REPLACE FUNCTION dsl.and(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(must=>dsl.must(VARIADIC queries));
$$;
CREATE OR REPLACE FUNCTION dsl.or(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(should=>dsl.should(VARIADIC queries));
$$;



--
-- vacuum and visibility function changes
--

CREATE OR REPLACE FUNCTION zdb.vac_by_xmin(index regclass, type text, xmin bigint) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
 * docs with aborted xmins
 */
    SELECT dsl.and(
        dsl.range(field=>'zdb_xmin', lt=>xmin),
        dsl.terms_lookup('zdb_xmin', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')
    );
$$;

CREATE OR REPLACE FUNCTION zdb.vac_by_xmax(index regclass, type text, xmax bigint) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
 * docs with committed xmax
 */
    SELECT dsl.and(
        dsl.range(field=>'zdb_xmax', lt=>xmax),
        dsl.noteq(dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids'))
    );
$$;

CREATE OR REPLACE FUNCTION zdb.vac_aborted_xmax(index regclass, type text, xmax bigint) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
 * docs with aborted xmax
 */
    SELECT dsl.and(
        dsl.range(field=>'zdb_xmax', lt=>xmax),
        dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')
    );
$$;

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

  SELECT dsl.and(
      dsl.noteq('_id:zdb_aborted_xids'),
      dsl.or(
          dsl.and(
              dsl.terms('zdb_xmin', VARIADIC myXid),
              dsl.range(field => 'zdb_cmin', lt => myCid),
              dsl.or(
                  dsl.field_missing('zdb_xmax'),
                  dsl.and(
                      dsl.terms('zdb_xmax', VARIADIC myXid),
                      dsl.range(field => 'zdb_cmax', gte => myCid)
                  )
              )
          ),
          dsl.and(
              dsl.and(
                  dsl.noteq(dsl.terms_lookup('zdb_xmin', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')),
                  dsl.noteq(dsl.terms('zdb_xmin', VARIADIC active_xids)),
                  dsl.noteq(dsl.range(field => 'zdb_xmin', gte => myXmax)),
                  dsl.or(
                      dsl.field_missing('zdb_xmax'),
                      dsl.and(
                          dsl.terms('zdb_xmax', VARIADIC myXid),
                          dsl.range(field => 'zdb_cmax', gte => myCid)
                      ),
                      dsl.and(
                          dsl.noteq(dsl.terms('zdb_xmax', VARIADIC myXid)),
                          dsl.or(
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
