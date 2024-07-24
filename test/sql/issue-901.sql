CREATE TABLE issue901_main
(
    pk_m_id   SERIAL8 NOT NULL PRIMARY KEY,
    m_name    text    NOT NULL,
    m_date    timestamp,
    m_fk_to_v bigint[]
);

CREATE TABLE issue901_var
(
    pk_v_id SERIAL8 NOT NULL PRIMARY KEY,
    v_state varchar
);

CREATE OR REPLACE VIEW issue901_view AS
SELECT issue901_main.*,
       (SELECT array_agg(v.v_state)
        FROM issue901_var v
        WHERE v.pk_v_id = ANY (issue901_main.m_fk_to_v)) AS v_data,
       issue901_main.*::issue901_main                    AS zdb
FROM issue901_main;

CREATE INDEX idx901_var ON issue901_var USING zombodb ((issue901_var.*)) WITH (replicas='1', shards='5');
CREATE INDEX idx901_main ON issue901_main USING zombodb ((issue901_main.*)) WITH (options='v_data:(m_fk_to_v=<public.issue901_var.idx901_var>pk_v_id)', replicas='1', shards='5');

INSERT INTO issue901_main (m_name, m_date, m_fk_to_v)
values ('Jupiter', '2001-01-01', ARRAY [4]);
INSERT INTO issue901_main (m_name, m_date, m_fk_to_v)
values ('Saturn', '2020-05-05', ARRAY [3]);
INSERT INTO issue901_main (m_name, m_date, m_fk_to_v)
values ('Neptune', '2033-12-31', ARRAY [2]);
INSERT INTO issue901_main (m_name, m_date, m_fk_to_v)
values ('Sirius', '1994-07-04', ARRAY [1,2]);

INSERT INTO issue901_var (v_state)
values ('happy');
INSERT INTO issue901_var (v_state)
values ('sad');
INSERT INTO issue901_var (v_state)
values ('not happy');
INSERT INTO issue901_var (v_state)
values ('heavy');

select *
from issue901_view;


SELECT m_name, m_date, v_data FROM issue901_view where zdb ==> '(v_state:"*" AND m_date > "2000-01-01") AND m_name = "s*"';


-- this should NOT include an extra term of "sad"
SELECT term, count
FROM zdb.tally('issue901_view'::regclass, 'v_data.v_state', 'FALSE', '^.*',
               '(v_state:"*" AND m_date > "2000-01-01") AND m_name = "s*"'::zdbquery, 2147483647, 'term'::termsorderby);

-- sql version of above
select v_state, count(*)
from issue901_var, issue901_main
where pk_v_id = ANY(m_fk_to_v)
  and (v_state is not null
  and m_date > '2000-01-01')
  and m_name ilike 's%'
group by 1;


-- without parens in the query, it works!
SELECT term, count
FROM zdb.tally('issue901_view'::regclass, 'v_data.v_state', 'FALSE', '^.*',
               'v_state:"*" AND m_date > "2000-01-01" AND m_name = "s*"'::zdbquery, 2147483647, 'term'::termsorderby);


--
-- validate the `pullup_and` function does its job
select ast from zdb.debug_query('issue901_view', 'field:(a (other:(b other2:(c1 other3:(c2 c2_1 c2_2) c3)) foo:(d bar:e f)))');


--
-- these are all correct
--

SELECT m_name, m_date, v_data
FROM issue901_view
where zdb ==> '(v_state:"*" AND m_date > "1980-01-01") AND m_name = "s*"';
SELECT term, count
FROM zdb.tally('issue901_view'::regclass, 'v_data.v_state', 'FALSE', '^.*',
               '(v_state:"*" AND m_date > "1980-01-01") AND m_name = "s*"'::zdbquery, 2147483647, 'term'::termsorderby);

SELECT m_name, m_date, v_data
FROM issue901_view
where zdb ==> 'v_state:"*" AND m_date > "2000-01-01" AND m_name = "s*"';
SELECT term, count
FROM zdb.tally('issue901_view'::regclass, 'v_data.v_state', 'FALSE', '^.*',
               'v_state:"*" AND m_date > "2000-01-01" AND m_name = "s*"'::zdbquery, 2147483647, 'term'::termsorderby);

SELECT m_name, m_date, v_data
FROM issue901_view
where zdb ==> 'm_name = "s*" AND (v_state:"*" AND m_date > "2000-01-01")';
SELECT term, count
FROM zdb.tally('issue901_view'::regclass, 'v_data.v_state', 'FALSE', '^.*',
               'm_name = "s*" AND (v_state:"*" AND m_date > "2000-01-01")'::zdbquery, 2147483647, 'term'::termsorderby);

DROP TABLE issue901_main CASCADE;
DROP TABLE issue901_var CASCADE;