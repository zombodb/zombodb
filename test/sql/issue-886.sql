CREATE TABLE issue886 (
    pk_id bigint,
    full_text text
);

INSERT INTO issue886 (pk_id, full_text) VALUES (4, ' Bing Crosby was a singer known as a "crooner" famous for popular Christmas songs. In 1962, he formed the band CROSBY, STILLS, NASH & YOUNG who played at Woodstock. He was also captain of the Pittsburgh Penguins NHL hockey team. Most people are familiar with an animated kids show he created, called Fat Albert and the crosby kids.');
CREATE INDEX idxissue886 ON issue886 USING zombodb ((issue886.*));

WITH matches as MATERIALIZED (
    SELECT * FROM issue886 WHERE pk_id = 4),
     highlights AS MATERIALIZED (
         SELECT (
                    zdb.highlight_document('issue886'::regclass,
                                           json_build_object('full_text',full_text),
                                           'full_text:(("Bing" or "Fat") w/2 "crosby")'::TEXT)
                    ).*,
                pk_id AS primary_key FROM matches
     )
SELECT * FROM highlights;

WITH matches as MATERIALIZED (
    SELECT * FROM issue886 WHERE pk_id = 4),
     highlights AS MATERIALIZED (
         SELECT (
                    zdb.highlight_document('issue886'::regclass,
                                           json_build_object('full_text',full_text),
                                           'full_text:(("Bing","Fat") w/2 "crosby")'::TEXT)
                    ).*,
                pk_id AS primary_key FROM matches
     )
SELECT * FROM highlights;

WITH matches as MATERIALIZED (
    SELECT * FROM issue886 WHERE pk_id = 4),
     highlights AS MATERIALIZED (
         SELECT (
                    zdb.highlight_document('issue886'::regclass,
                                           json_build_object('full_text',full_text),
                                           'full_text:(("Bing" or "David") w/2 "crosby")'::TEXT)
                    ).*,
                pk_id AS primary_key FROM matches
     )
SELECT * FROM highlights;

WITH matches as MATERIALIZED (
    SELECT * FROM issue886 WHERE pk_id = 4),
     highlights AS MATERIALIZED (
         SELECT (
                    zdb.highlight_document('issue886'::regclass,
                                           json_build_object('full_text',full_text),
                                           'full_text:(("David" or "Bing") w/2 "crosby")'::TEXT)
                    ).*,
                pk_id AS primary_key FROM matches
     )
SELECT * FROM highlights;

WITH matches as MATERIALIZED (
    SELECT * FROM issue886 WHERE pk_id = 4),
     highlights AS MATERIALIZED (
         SELECT (
                    zdb.highlight_document('issue886'::regclass,
                                           json_build_object('full_text',full_text),
                                           'full_text:(("David","Bing") w/2 "crosby")'::TEXT)
                    ).*,
                pk_id AS primary_key FROM matches
     )
SELECT * FROM highlights;

WITH matches as MATERIALIZED (
    SELECT * FROM issue886 WHERE pk_id = 4),
     highlights AS MATERIALIZED (
         SELECT (
                    zdb.highlight_document('issue886'::regclass,
                                           json_build_object('full_text',full_text),
                                           'full_text:(("Bing" or "David" or "Fat") w/2 "crosby")'::TEXT)
                    ).*,
                pk_id AS primary_key FROM matches
     )
SELECT * FROM highlights;
DROP TABLE issue886 CASCADE;