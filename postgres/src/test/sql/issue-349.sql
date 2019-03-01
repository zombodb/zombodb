create table issue349 (
  id serial8 not null primary key,
  data text
);

SELECT zdb_define_filter('issue349_ngram_filter', '{
  "type": "edgeNGram",
  "min_gram": 3,
  "max_gram": 8
}');
SELECT zdb_define_analyzer('issue349_analyzer', '{
  "type": "custom",
  "tokenizer": "standard",
  "filter": ["lowercase", "asciifolding","issue349_ngram_filter"]
}');

SELECT zdb_define_mapping('issue349', 'data', '{
  "store": false,
  "type": "text",
  "fielddata": true,
  "index_options": "positions",
  "include_in_all": "true",
  "analyzer": "issue349_analyzer",
  "fields": {
    "exact": {
      "type": "keyword"
    }
  }
}');

create index idxissue349 on issue349 using zombodb (zdb('issue349', ctid), zdb(issue349)) with (url='localhost:9200/');

insert into issue349 (data) values ('jeffrey bob david eric sam california');

select * from issue349 where zdb('issue349', ctid) ==> 'data:jeff';
select * from issue349 where zdb('issue349', ctid) ==> 'data:j*';
select * from issue349 where zdb('issue349', ctid) ==> 'data:jeffrey';
select * from issue349 where zdb('issue349', ctid) ==> 'data:"bob david"';

drop table issue349;
