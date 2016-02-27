CREATE TABLE analyzers_test (
  arabic     arabic,
  armenian   armenian,
  basque     basque,
  brazilian  brazilian,
  bulgarian  bulgarian,
  catalan    catalan,
  chinese    chinese,
  cjk        cjk,
  czech      czech,
  danish     danish,
  dutch      dutch,
  english    english,
  finnish    finnish,
  french     french,
  galician   galician,
  german     german,
  greek      greek,
  hindi      hindi,
  hungarian  hungarian,
  indonesian indonesian,
  irish      irish,
  italian    italian,
  latvian    latvian,
  norwegian  norwegian,
  persian    persian,
  portuguese portuguese,
  romanian   romanian,
  russian    russian,
  sorani     sorani,
  spanish    spanish,
  swedish    swedish,
  turkish    turkish,
  thai       thai
);


INSERT INTO analyzers_test VALUES (
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
  , 'นี่คือการทดสอบ');

CREATE INDEX idxanalyzers_test ON analyzers_test USING zombodb (zdb('analyzers_test', ctid), zdb_to_jsonb(analyzers_test)) WITH (url='http://localhost:9200/');
SELECT
  attname,
  (SELECT zdb('analyzers_test', ctid) ==> (attname || ':test') :: TEXT
   FROM analyzers_test) found
FROM pg_attribute
WHERE attrelid = 'analyzers_test' :: REGCLASS AND attnum >= 1
ORDER BY attnum;

SELECT zdb_analyze_text('idxanalyzers_test', attname, (SELECT row_to_json(analyzers_test) ->> attname FROM analyzers_test))
FROM pg_attribute
WHERE attrelid = 'analyzers_test' :: REGCLASS AND attnum >= 1
        AND attname <> 'persian' /* difference in output between OS X and Linux -- easier to just ignore it */
ORDER BY attnum;

SELECT
  attname,
  q
FROM analyzers_test, (SELECT
                        attname,
                        (attname || ':' || ((SELECT row_to_json(analyzers_test)
                                             FROM analyzers_test) -> attname)) :: TEXT q
                      FROM pg_attribute
                      WHERE attrelid = 'analyzers_test' :: REGCLASS AND attnum >= 1) x
WHERE zdb('analyzers_test', ctid) ==> q
ORDER BY attname;

DROP TABLE analyzers_test;