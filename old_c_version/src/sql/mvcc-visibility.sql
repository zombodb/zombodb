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

CREATE OR REPLACE FUNCTION zdb.internal_visibility_clause(index regclass) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_visibility_clause';
