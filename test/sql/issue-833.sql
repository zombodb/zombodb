CREATE TABLE issue833 (
                                    pk bigint NOT NULL,
                                    pizza json
);

CREATE INDEX idxissue833 ON issue833
    USING zombodb ((issue833.*));

INSERT INTO issue833 values (1, '[{"topping":"pepperoni"},{"tastiness":"great"}]');
INSERT INTO issue833 values (2, '[{"topping":"mushroom"},{"tastiness":"good"}]');
INSERT INTO issue833 values (3, '[{"topping":"mozzarella"},{"tastiness":"necessary"}]');
INSERT INTO issue833 values (4, '[{"topping":"pineapple"},{"tastiness":"n/a"}]');

SELECT * FROM zdb.tally('issue833','pizza.topping',True,'^.*','') limit 10;
SELECT * FROM zdb.tally('issue833','pizza.topping',True,'^p.*','') limit 10;
SELECT * FROM zdb.tally('issue833','pizza.topping',True,'^pe.*','pizza.topping = "p*"') limit 10;

DROP TABLE issue833 CASCADE;