CREATE SCHEMA test_cart_v2;

CREATE TABLE test_cart_v2.cart (
                                   pk_cart serial8,
                                   my_varchar varchar,
                                   my_text text,
                                   CONSTRAINT idx_test_cart_v2_cart_pk_cart PRIMARY KEY (pk_cart));

--CREATE ES INDEX
CREATE INDEX es_test_cart_v2_cart ON test_cart_v2.cart USING zombodb ((test_cart_v2.cart.*)) WITH (shards='5', replicas='0', max_analyze_token_count='10000000', max_terms_count='2147483647');

--CREATE VIEW
CREATE VIEW test_cart_v2.cart_view AS
SELECT
    *,
    cart.*::test_cart_v2.cart AS zdb
FROM test_cart_v2.cart;

--CREATE DATA
INSERT INTO test_cart_v2.cart(my_varchar,my_text) VALUES('cat', 'my_name is tom');
INSERT INTO test_cart_v2.cart(my_varchar,my_text) VALUES('fire', 'my_name is bob');

--CREATE CRITERIA TABLE
CREATE TABLE test_cart_v2.criteria (
                                       pk_crit serial8,
                                       my_crit text,
                                       CONSTRAINT idx_test_cart_v2_criteria_pk_crit PRIMARY KEY (pk_crit));

INSERT INTO test_cart_v2.criteria(my_crit) VALUES('my_varchar:"cat"');
INSERT INTO test_cart_v2.criteria(my_crit) VALUES('my_text:"bob"');
INSERT INTO test_cart_v2.criteria(my_crit) VALUES('my_text:"mary"');
INSERT INTO test_cart_v2.criteria(my_crit) VALUES('my_varchar:"dog"');

--test SQL
select m.pk_cart, my_varchar,my_text,t.my_crit, temp_query
from (select *,'pk_cart:"*" AND '||my_crit as temp_query
      from test_cart_v2.criteria where pk_crit IN(2,3)) t,
     test_cart_v2.cart_view m where zdb ==>temp_query;

DROP SCHEMA test_cart_v2 CASCADE;