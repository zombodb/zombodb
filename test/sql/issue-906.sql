DROP SCHEMA IF EXISTS jointest CASCADE;
CREATE SCHEMA jointest;

CREATE TABLE jointest.alpha_dat
(
    a_id  SERIAL8 NOT NULL PRIMARY KEY,
    a_foo varchar
);

CREATE INDEX es_idxalpha ON jointest.alpha_dat USING zombodb ((jointest.alpha_dat.*));

CREATE TABLE jointest.beta_dat
(
    b_id    SERIAL8 NOT NULL PRIMARY KEY,
    b_fk    int8,
    b_one   varchar,
    b_two   varchar,
    b_three varchar
);

CREATE INDEX es_idxbeta ON jointest.beta_dat USING zombodb ((jointest.beta_dat.*));


INSERT INTO jointest.alpha_dat (a_foo)
    (SELECT 'TEST-' || s.a AS a_foo FROM generate_series(6, 5000, 1) AS s(a));

INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (1, 'sample', 'green', 'fuzzy');
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (2, 'zipper', 'blue', 'metallic');
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (3, 'umbrella', 'red', 'spiked');
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (3, 'roach spray', 'green', 'spiked');
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (1, null, 'green', 'slippery');
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (7, 'house', 'brown', 'night');
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (10, 'pancake', 'orange', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (11, 'syrup', 'yellow', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (11, 'foam', 'black', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (11, 'ladder', 'silver', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (12, 'pedal', 'gold', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (13, 'tulip', 'magenta', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (14, 'spiral', 'aqua', null);
INSERT INTO jointest.beta_dat(b_fk, b_one, b_two, b_three)
VALUES (15, 'skidmark', 'ecru', null);

CREATE VIEW jointest.alpha_view AS
SELECT alpha_dat.a_id,
       alpha_dat.a_foo,
       alpha_dat.*::jointest.alpha_dat AS zdb
FROM jointest.alpha_dat;

CREATE VIEW jointest.beta_view AS
SELECT beta_dat.*,
       beta_dat.*::jointest.beta_dat AS zdb
FROM jointest.beta_dat;


SELECT "BETA_b_id", "ALPHA_a_id"
FROM (SELECT "jointest"."beta_view"."b_id" AS "BETA_b_id", "jointest"."alpha_view"."a_id" AS "ALPHA_a_id"
      FROM "jointest"."beta_view"
               INNER JOIN "jointest"."alpha_view"
                          ON "jointest"."beta_view"."b_fk" = "jointest"."alpha_view"."a_id"
      WHERE (
                (("jointest"."beta_view".zdb ==> 'b_three: null'))
                    AND
                ((
                    "jointest"."alpha_view".zdb ==> 'a_foo: null'
                        AND "jointest"."beta_view".zdb ==> 'b_two = "GREEN"'
                        OR ("jointest"."beta_view".zdb ==> 'b_one = "*"')
                    ))
                )) x;


DECLARE curs99 SCROLL CURSOR WITH HOLD FOR
    SELECT "BETA_b_id", "ALPHA_a_id"
    FROM (SELECT "jointest"."beta_view"."b_id" AS "BETA_b_id", "jointest"."alpha_view"."a_id" AS "ALPHA_a_id"
          FROM "jointest"."beta_view"
                   INNER JOIN "jointest"."alpha_view"
                              ON "jointest"."beta_view"."b_fk" = "jointest"."alpha_view"."a_id"
          WHERE (
                    (("jointest"."beta_view".zdb ==> 'b_three: null'))
                        AND
                    ((
                        "jointest"."alpha_view".zdb ==> 'a_foo: null'
                            AND "jointest"."beta_view".zdb ==> 'b_two = "GREEN"'
                            OR ("jointest"."beta_view".zdb ==> 'b_one = "*"')
                        ))
                    )) x;

FETCH 10 FROM curs99;


set enable_hashjoin to off;
set enable_mergejoin to off;
SELECT "BETA_b_id", "ALPHA_a_id"
FROM (SELECT "jointest"."beta_view"."b_id" AS "BETA_b_id", "jointest"."alpha_view"."a_id" AS "ALPHA_a_id"
      FROM "jointest"."beta_view"
               INNER JOIN "jointest"."alpha_view"
                          ON "jointest"."beta_view"."b_fk" = "jointest"."alpha_view"."a_id"
      WHERE (
                (("jointest"."beta_view".zdb ==> 'b_three: null'))
                    AND
                ((
                    "jointest"."alpha_view".zdb ==> 'a_foo: null'
                        AND "jointest"."beta_view".zdb ==> 'b_two = "GREEN"'
                        OR ("jointest"."beta_view".zdb ==> 'b_one = "*"')
                    ))
                )) x;


DROP SCHEMA jointest CASCADE;