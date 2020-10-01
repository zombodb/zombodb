create table issue349 (
  id serial8 not null primary key,
  data text
);

SELECT zdb.define_filter('issue349_ngram_filter', '{
  "type": "edgeNGram",
  "min_gram": 3,
  "max_gram": 8
}');
SELECT zdb.define_analyzer('issue349_analyzer', '{
  "type": "custom",
  "tokenizer": "standard",
  "filter": ["lowercase", "asciifolding","issue349_ngram_filter"]
}');

SELECT zdb.define_field_mapping('issue349', 'data', '{
  "store": false,
  "type": "text",
  "fielddata": true,
  "index_options": "positions",
  "copy_to": [ "zdb_all" ],
  "analyzer": "issue349_analyzer",
  "fields": {
    "exact": {
      "type": "keyword"
    }
  }
}');

create index idxissue349 on issue349 using zombodb ( (issue349.*) );

insert into issue349 (data) values ('jeffrey bob david eric sam california');

select * from issue349 where issue349 ==> 'data:jeff';
select * from issue349 where issue349 ==> 'data:j*';
select * from issue349 where issue349 ==> 'data:jeffrey';
select * from issue349 where issue349 ==> 'data:"bob david"';

drop table issue349;
