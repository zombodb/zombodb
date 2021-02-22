CREATE TABLE issue632 (
                       id serial8,
                       name zdb.phrase,
                       email varchar,
                       password varchar,
                       created timestamp
);

INSERT INTO issue632 (name, email, password, created) VALUES
('Joe Doe', 'joe.doe@gmail.com', '123joe', now()),
('Tom Doe', 'tom.doe@gmail.com', '123joe', now()),
('Tammy Doe', 'tammy.doe@gmail.com', '123joe', now()),
('Fan Doe', 'fan.doe@gmail.com', '123joe', now()),
('Billie Doe', 'billie.doe@gmail.com', '123joe', now());

CREATE TYPE issue632_idx_type AS (
                                 id bigint,
                                 name zdb.phrase,
                                 email varchar,
                                 created timestamp
                             );

CREATE FUNCTION issue632_idx_func(issue632) RETURNS issue632_idx_type IMMUTABLE STRICT LANGUAGE sql AS $$
SELECT ROW (
           $1.id,
           $1.name,
           $1.email,
           $1.created
           )::issue632_idx_type
$$;

-- NOTE: we update the row Before creating the index
UPDATE issue632 SET name = 'Jimmy Donover', email = 'jimmy.donover@gmail.com' where id = 4;

-- Then create index
CREATE INDEX idxissue632
    ON issue632
        USING zombodb ((issue632_idx_func(issue632.*)));

select id, name, email, password from issue632 order by ctid;
select zdb.score(ctid), id, name, email, password from issue632 where issue632 ==> '*' order by ctid;

DROP TABLE issue632 CASCADE;
DROP TYPE issue632_idx_type;
