CREATE TABLE public.students
(
  pk_stu bigserial,
  stu_first_name varchar,
  stu_last_name varchar,
  PRIMARY KEY (pk_stu)
);

CREATE INDEX es_students ON public.students USING zombodb ( (students.*) ) WITH (shards='3', replicas='1');

INSERT INTO public.students VALUES(DEFAULT, 'Robert', 'Smith');
INSERT INTO public.students VALUES(DEFAULT, 'Mary', 'Jane');
INSERT INTO public.students VALUES(DEFAULT, 'Alice', 'Jones');
INSERT INTO public.students VALUES(DEFAULT, 'Tommy', 'Gunn');
INSERT INTO public.students VALUES(DEFAULT, 'Walter', 'Smith');


CREATE TABLE public.books
(
  pk_book bigserial,
  book_name varchar,
  book_isbn varchar,
  PRIMARY KEY (pk_book)
);

CREATE INDEX es_books ON public.books USING zombodb ( (books.*) ) WITH (shards='3', replicas='1');

INSERT INTO public.books VALUES(DEFAULT, 'Algebra', '0000000000021');
INSERT INTO public.books VALUES(DEFAULT, 'Cooking', '0000000000062');
INSERT INTO public.books VALUES(DEFAULT, 'Physics', '0000000000083');
INSERT INTO public.books VALUES(DEFAULT, 'Advanced Baking', '0000000000054');
INSERT INTO public.books VALUES(DEFAULT, 'Art History', '0000000000025');
INSERT INTO public.books VALUES(DEFAULT, 'SQL Injection', '0000000000036');
INSERT INTO public.books VALUES(DEFAULT, 'Statistics', '0000000000127');
INSERT INTO public.books VALUES(DEFAULT, 'Business Administration', '0000000000456');
INSERT INTO public.books VALUES(DEFAULT, 'Anatomy', '0000000000978');
INSERT INTO public.books VALUES(DEFAULT, 'Calculus', '0000000001234');
INSERT INTO public.books VALUES(DEFAULT, 'Biology', '0000000000007');



CREATE TABLE public.courses
(
  pk_cse bigserial,
  cse_name varchar,
  fk_cse_book bigint[],
  fk_cse_room bigint,
  fk_cse_prof bigint,
  fk_cse_stu bigint[],
  PRIMARY KEY (pk_cse)
);

CREATE INDEX es_courses ON public.courses USING zombodb ( (courses.*) ) WITH (shards='3', replicas='1');

INSERT INTO public.courses VALUES(DEFAULT, 'Introduction to S.P.E.C.T.R.E', ARRAY[3, 5, 10], 1, 1, ARRAY[2, 3]);
INSERT INTO public.courses VALUES(DEFAULT, 'Early Hominides', ARRAY[9, 11], 2, 2, ARRAY[1, 2, 3, 4, 5]);
INSERT INTO public.courses VALUES(DEFAULT, 'Cookies to Souffle', ARRAY[2, 4], 3, 3, ARRAY[1, 3, 5]);
INSERT INTO public.courses VALUES(DEFAULT, 'Web Hacking 101', ARRAY[6], 4, 3, ARRAY[2, 4]);
INSERT INTO public.courses VALUES(DEFAULT, 'Pre-MBA Studies', ARRAY[1, 8], 4, 1, ARRAY[1, 2, 4]);


--SQL RETURNS CORRECT ANSWER(empty result set)
SELECT *
FROM public.students
WHERE pk_stu IN (SELECT unnest(fk_cse_stu)
                 FROM public.courses
                 WHERE cse_name ILIKE 'intro%' AND fk_cse_book && (SELECT array_agg(pk_book)
                                                                   FROM public.books
                                                                   WHERE book_name ILIKE '%cook%'));

--ZDB RETURNS CORRECT ANSWER(empty result set)
SELECT *
FROM public.students
WHERE students ==> dsl.link_options(ARRAY ['pk_stu=<public.courses.es_courses>fk_cse_stu',
                                        'fk_cse_book=<public.books.es_books>pk_book'],
                                    'cse_name:"intro*" AND book_name:"*cook*"');


--ZDB WITH NAMED INDEX LINKS RETURNS WRONG ANSWER(Alice)
-- appears to be broken when using named index links and criteria is on linked tables
SELECT *
FROM public.students
WHERE students ==> dsl.link_options(ARRAY ['courses:(pk_stu=<public.courses.es_courses>fk_cse_stu)',
                                        'books:(courses.fk_cse_book=<public.books.es_books>pk_book)'],
                                    'cse_name:"intro*" AND book_name:"*cook*"');

-- returns two results, via zdb
SELECT *
FROM public.students
WHERE students ==>
      dsl.link_options(
              ARRAY ['courses:(pk_stu=<public.courses.es_courses>fk_cse_stu)', 'books:(courses.fk_cse_book=<public.books.es_books>pk_book)'],
              'cse_name:"intro*"')
ORDER BY pk_stu;

DROP TABLE students CASCADE;
DROP TABLE books CASCADE;
DROP TABLE courses CASCADE;

