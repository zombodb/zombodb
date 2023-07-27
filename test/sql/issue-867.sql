CREATE TABLE main
(
    id   bigint,
    name varchar
);

CREATE TABLE other
(
    other_id bigint,
    fk_main  bigint[],
    a        varchar,
    b        varchar[],
    c        varchar,
    d        varchar[]
);

CREATE VIEW other_view AS
SELECT other.other_id,
       other.fk_main,
       other.a,
       other.b,
       other.c,
       other.d,
       (select array_agg(name) from main where id = ANY (fk_main)) name,
       other as                                                    zdb
FROM other;

CREATE INDEX idxmain ON main USING zombodb ((main.*));
CREATE INDEX idxother ON other USING zombodb ((other.*)) WITH (options = 'fk_main=<public.main.idxmain>id');

INSERT INTO main (id, name)
VALUES (1, 'Brandy');

-- insert a record that would match the query if the outer NOT wasn't there
INSERT INTO other (other_id, fk_main, a, b, c, d)
VALUES (100, '{1}', 'A', '{B}', 'C', '{D}');

-- insert a record that matches the query as it's written
INSERT INTO other (other_id, fk_main, a, b, c, d)
VALUES (100, '{1}', 'not_A', '{B}', 'C', '{not_D}');

SELECT a, b, c, d, fk_main, name
FROM other_view
WHERE

              NOT
                  (
                      a = 'A' OR a = 'Y' OR a = 'YES'
                  )
              AND
                  (
                      'B' = ANY (b) AND (c = 'C' OR c = 'YES' OR c IS NULL) AND 'D' <> ANY (d)
                  )
ORDER BY other_id;

SELECT *
FROM zdb.tally('other_view', 'name', false, '^b.*', '( (NOT ((a: "A" OR a: Y OR
                                                     a: YES) )) AND
                                                     (((b = B AND
                                                          (c = C OR c = YES OR c = NULL)
                                                     AND d <> D))))', 10, 'term');

DROP VIEW other_view;
DROP TABLE other;
DROP TABLE main;
