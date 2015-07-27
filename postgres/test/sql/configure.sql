CREATE OR REPLACE FUNCTION assert(msg text, expected anyelement, actual anyelement) RETURNS anyelement LANGUAGE plpgsql AS $$
BEGIN
	IF expected IS DISTINCT FROM actual THEN
		RAISE EXCEPTION '%: expect:%, got:%', msg, expected, actual;
	END IF;
	RETURN expected;
END;
$$;

CREATE OR REPLACE FUNCTION assert(msg text, expected integer, actual bigint) RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
        IF expected::bigint IS DISTINCT FROM actual THEN
                RAISE EXCEPTION '%: expect:%, got:%', msg, expected, actual;
        END IF;
        RETURN expected;
END;
$$;
