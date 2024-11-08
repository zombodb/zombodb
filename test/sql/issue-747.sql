CREATE TABLE testtally_main
(
    id   SERIAL8 NOT NULL PRIMARY KEY,
    name text    NOT NULL
);


CREATE TABLE testtally_var
(
    id    SERIAL8 NOT NULL PRIMARY KEY,
    state varchar
);

CREATE OR REPLACE VIEW testtallyview AS
SELECT testtally_main.*, testtally_var.state, testtally_main AS zdb
FROM testtally_main
         LEFT JOIN testtally_var ON testtally_main.id = testtally_var.id;

CREATE INDEX es_idxtesttally_var ON testtally_var USING zombodb ((testtally_var.*));
CREATE INDEX es_idxtesttally_main ON testtally_main USING zombodb ((testtally_main.*)) WITH (options ='id = <public.testtally_var.es_idxtesttally_var>id');

INSERT INTO testtally_main (name)
values ('Jupiter');
INSERT INTO testtally_main (name)
values ('Saturn');
INSERT INTO testtally_main (name)
values ('Neptune');
INSERT INTO testtally_main (name)
values ('Sirius');

INSERT INTO testtally_var (state)
values ('happy');
INSERT INTO testtally_var (state)
values ('happy');
INSERT INTO testtally_var (state)
values ('not happy');
INSERT INTO testtally_var (state)
values ('heavy');

SELECT term, count FROM zdb.tally('public.testtallyview','state',false,'^.*','(name = "s*")',2147483647,'term');

DROP TABLE testtally_var CASCADE;
DROP TABLE testtally_main CASCADE;