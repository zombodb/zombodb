CREATE OR REPLACE FUNCTION vac_by_xmin(index regclass, type text, xmin bigint) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
 * docs with aborted xmins
 */
    SELECT dsl.must(
        dsl.range(field=>'zdb_xmin', lt=>xmin),
        dsl.filter(dsl.terms_lookup('zdb_xmin', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids'))
    );
$$;

CREATE OR REPLACE FUNCTION vac_by_xmax(index regclass, type text, xmax bigint) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
 * docs with committed xmax
 */
    SELECT dsl.must(
        dsl.range(field=>'zdb_xmax', lt=>xmax),
        dsl.filter(dsl.noteq(dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids')))
    );
$$;

CREATE OR REPLACE FUNCTION vac_aborted_xmax(index regclass, type text, xmax bigint) RETURNS zdbquery PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
/*
 * docs with aborted xmax
 */
    SELECT dsl.must(
        dsl.range(field=>'zdb_xmax', lt=>xmax),
        dsl.filter(dsl.terms_lookup('zdb_xmax', zdb.index_name(index), type, 'zdb_aborted_xids', 'zdb_aborted_xids'))
    );
$$;
