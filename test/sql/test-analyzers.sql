CREATE TABLE analyzers_test (
  pkey       serial8 NOT NULL PRIMARY KEY,
  arabic     zdb.arabic,
  armenian   zdb.armenian,
  basque     zdb.basque,
  brazilian  zdb.brazilian,
  bulgarian  zdb.bulgarian,
  catalan    zdb.catalan,
  chinese    zdb.chinese,
  cjk        zdb.cjk,
  czech      zdb.czech,
  danish     zdb.danish,
  dutch      zdb.dutch,
  english    zdb.english,
  finnish    zdb.finnish,
  french     zdb.french,
  galician   zdb.galician,
  german     zdb.german,
  greek      zdb.greek,
  hindi      zdb.hindi,
  hungarian  zdb.hungarian,
  indonesian zdb.indonesian,
  irish      zdb.irish,
  italian    zdb.italian,
  latvian    zdb.latvian,
  norwegian  zdb.norwegian,
  persian    zdb.persian,
  portuguese zdb.portuguese,
  romanian   zdb.romanian,
  russian    zdb.russian,
  sorani     zdb.sorani,
  spanish    zdb.spanish,
  swedish    zdb.swedish,
  turkish    zdb.turkish,
  thai       zdb.thai,
  fulltext_with_shingles zdb.fulltext_with_shingles,
  zdb_standard zdb.zdb_standard,
  whitespace zdb.whitespace
);


INSERT INTO analyzers_test VALUES (
  DEFAULT,
  'هذا اختبار'
  , 'սա փորձություն է'
  , 'hau froga bat da'
  , 'esto es un exámen' -- brazilian
  , 'това е тест'
  , 'Això és un examen'
  , '这是一个测试'
  , 'これはテストです' -- cjk (japanese)
  , 'toto je test'
  , 'dette er en test'
  , 'dit is een test'
  , 'this is a test' -- english
  , 'Tämä on koe'
  , 'c''est un test'
  , 'este é unha proba de'
  , 'das ist ein Test' -- german
  , 'αυτό είναι ένα τεστ'
  , 'यह एक परीक्षण है' -- hindi
  , 'ez egy teszt'
  , 'ini adalah sebuah ujian'
  , 'tá sé seo le tástáil'
  , 'questa è una prova'
  , 'Šis ir tests' -- latvian
  , 'dette er en test' -- norwegian
  , 'این یک امتحان است' -- persian
  , 'isso é um teste'
  , 'acesta este un test'
  , 'Это тест' -- russian
  , 'this IS a test' -- sorani: couldn't find translation
  , 'esto es un exámen'
  , 'detta är ett prov'
  , 'bu bir test' -- turkish
  , 'นี่คือการทดสอบ'
  , 'this is a test' -- fulltext_with_shingles
  , 'this is a test' -- zdb_standard
  , 'this is a test' -- whitespace
  );

CREATE INDEX idxanalyzers_test ON analyzers_test USING zombodb ((analyzers_test));
SELECT
  attname,
  (SELECT count(*) > 0 FROM analyzers_test WHERE analyzers_test ==> term(attname, 'test')) found
FROM pg_attribute
WHERE attrelid = 'analyzers_test'::regclass AND attnum >= 1 AND attname <> 'pkey'
ORDER BY attnum;

SELECT
  attname,
  q
FROM analyzers_test, (SELECT
                        attname,
                        (attname || ':' || ((SELECT row_to_json(analyzers_test)
                                             FROM analyzers_test) -> attname))::text q
                      FROM pg_attribute
                      WHERE attrelid = 'analyzers_test'::regclass AND attnum >= 1 AND attname <> 'pkey') x
WHERE analyzers_test ==> q
ORDER BY attname;

DROP TABLE analyzers_test;