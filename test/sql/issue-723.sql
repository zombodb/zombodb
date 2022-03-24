CREATE TABLE testwith_main
(
    id   SERIAL8 NOT NULL PRIMARY KEY,
    name text    NOT NULL
);


CREATE TABLE testwith_vol
(
    id   SERIAL8 NOT NULL PRIMARY KEY,
    data json
);

CREATE
OR REPLACE VIEW testwithview AS
SELECT testwith_main.*, testwith_vol.data, testwith_main AS zdb
FROM testwith_main
         LEFT JOIN testwith_vol ON testwith_main.id = testwith_vol.id;

CREATE INDEX es_idxtestwith_vol ON testwith_vol USING zombodb ((testwith_vol.*));
CREATE INDEX es_idxtestwith_main ON testwith_main USING zombodb ((testwith_main.*)) WITH (options ='id = <public.testwith_vol.es_idxtestwith_vol>id');

INSERT INTO testwith_main (name)
values ('Jupiter');
INSERT INTO testwith_main (name)
values ('Saturn');
INSERT INTO testwith_main (name)
values ('Neptune');

INSERT INTO testwith_vol (data)
values ('[
  {
    "ref": 1,
    "number": 100,
    "fruit": "apple",
    "meal": "dessert"
  }
]');
INSERT INTO testwith_vol (data)
values ('[
  {
    "ref": 1,
    "number": 200,
    "fruit": "orange",
    "meal": "breakfast"
  },
  {
    "ref": 2,
    "number": 100,
    "fruit": "pineapple",
    "meal": "luau"
  }
]');
INSERT INTO testwith_vol (data)
values ('[
  {
    "ref": 1,
    "number": 300,
    "fruit": "grapefruit",
    "meal": "breakfast"
  },
  {
    "ref": 3,
    "number": 100,
    "fruit": "pulled pork bbq",
    "meal": "anytime"
  }
]');

-- this should return 1 row, "breakfast"
select term, count from zdb.tally('testwith_vol'::regclass, 'data.meal', 'TRUE', '^.*', '(data.meal="breakfast" WITH data.fruit="orange")'::zdbquery);

-- as should this
select term, count from zdb.tally('testwithview'::regclass, 'data.meal', 'TRUE', '^.*', '(data.meal="breakfast" WITH data.fruit="orange")'::zdbquery);

-- this should return 1 row, "anytime"
select term, count from zdb.tally('testwith_vol'::regclass, 'data.meal', 'TRUE', '^.*', '(data.meal="anytime" WITH data.number="100")'::zdbquery);

-- as should this
select term, count from zdb.tally('testwithview'::regclass, 'data.meal', 'TRUE', '^.*', '(data.meal="anytime" WITH data.number="100")'::zdbquery);

DROP TABLE testwith_main, testwith_vol CASCADE;