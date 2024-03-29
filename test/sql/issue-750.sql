CREATE TABLE IF NOT EXISTS table_a (
                                       id serial primary key,
                                       val text
);
DROP TABLE IF EXISTS table_b;

CREATE TABLE IF NOT EXISTS table_b (
                                       id serial primary key,
                                       id_a integer references table_a(id),
                                       val text
);

alter table table_a add column id_b integer references table_b(id);



CREATE TYPE table_a_type AS (
                                id integer,
                                id_b integer,
                                val text
                            );

CREATE FUNCTION table_a_idx(table_a) RETURNS table_a_type IMMUTABLE STRICT LANGUAGE sql AS $$
SELECT ROW (
           $1.id,
           $1.id_b,
           $1.val
           )
$$;

CREATE INDEX table_a_idx
    ON table_a
        USING zombodb (table_a_idx(table_a));

CREATE TYPE table_b_type AS (
                                id integer,
                                val text
                            );

CREATE FUNCTION table_b_idx(table_b) RETURNS table_b_type IMMUTABLE STRICT LANGUAGE sql AS $$
SELECT ROW (
           $1.id,
           $1.val
           )
$$;

CREATE INDEX table_b_idx
    ON table_b
        USING zombodb (table_b_idx(table_b));

Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('hello');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');
Insert into table_a (val) values ('123');

BEGIN;
savepoint abc;
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');
insert into table_b (val) values ('world');

savepoint asdf;
delete from table_a where val = '123';
UPDATE table_a set id_b = 1 where val ='hello';
UPDATE table_b set id_a = 1 where val = 'world';
release savepoint asdf;
release savepoint abc;
COMMIT;

DROP function table_a_idx cascade;
DROP TYPE table_a_type cascade;
DROP function table_b_idx cascade;
DROP TYPE table_b_type cascade;

DROP TABLE table_a CASCADE;
DROP TABLE table_b CASCADE;
