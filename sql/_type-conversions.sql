--
-- custom type conversions for some built-in postgres types
--

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default)
VALUES ('point'::regtype, 'zdb.point_to_json'::regproc, true);
INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default)
VALUES ('point[]'::regtype, 'zdb.point_array_to_json'::regproc, true);

CREATE OR REPLACE FUNCTION zdb.bytea_to_json(bytea) RETURNS json
    PARALLEL SAFE IMMUTABLE STRICT
    LANGUAGE sql AS
$$
SELECT to_json(encode($1, 'base64'));
$$;

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default)
VALUES ('bytea'::regtype, 'zdb.bytea_to_json'::regproc, true);
