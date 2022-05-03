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
    "action": "hit",
    "card": {
      "suit": "diamonds",
      "value": "ace"
    }
  }
]');
INSERT INTO testwith_vol (data)
values ('[
  {
    "ref": 1,
    "number": 200,
    "action": "stand",
    "card": {
      "suit": "diamonds",
      "value": "two"
    }
  },
  {
    "ref": 2,
    "number": 100,
    "action": "hit",
    "card": {
      "suit": "hearts",
      "value": "jack"
    }
  }
]');
INSERT INTO testwith_vol (data)
values ('[
  {
    "ref": 1,
    "number": 300,
    "action": "hit",
    "card": {
      "suit": "clubs",
      "value": "ace"
    }
  },
  {
    "ref": 3,
    "number": 100,
    "action": "stand",
    "card": {
      "suit": "diamonds",
      "value": "taxi"
    }
  }
]');


SELECT *
FROM testwithview;
-- Tally is correct
SELECT term, count
FROM zdb.tally('testwithview'::regclass, 'data.number', 'TRUE', '^.*',
               '(data.action="hit" WITH data.card.suit="diamonds")'::zdbquery);

-- Query results appear incorrect
SELECT *
FROM testwithview
WHERE testwithview.zdb ==> '(data.action="hit" WITH data.card.suit="diamonds")';

-- Same-level nested seems to work for both tally and query results
SELECT term, count
FROM zdb.tally('testwithview'::regclass, 'data.number', 'TRUE', '^.*',
               '(data.card.value="ace" WITH data.card.suit="diamonds")'::zdbquery);
SELECT *
FROM testwithview
WHERE testwithview.zdb ==> '(data.card.value="ace" WITH data.card.suit="diamonds")';

-- Same-level nested seems to work for both tally and query results
SELECT term, count
FROM zdb.tally('testwithview'::regclass, 'data.number', 'TRUE', '^.*',
               '(data.action="hit" WITH data.number="100")'::zdbquery);
SELECT *
FROM testwithview
WHERE testwithview.zdb ==> '(data.action="hit" WITH data.number="100")';

DROP TABLE testwith_main, testwith_vol CASCADE;