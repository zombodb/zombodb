CREATE TABLE termlist_full AS SELECT * FROM zdb_termlist('so_posts', 'body', 'the', NULL, 1000);
CREATE TABLE termlist_incr AS SELECT * FROM termlist_full LIMIT 0;
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', NULL, 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);
INSERT INTO termlist_incr SELECT * FROM zdb_termlist('so_posts', 'body', 'the', (SELECT max(term) FROM termlist_incr), 25);

SELECT assert(count(*), 0, 'all terms match') FROM termlist_full f LEFT JOIN termlist_incr i ON f.term = i.term
   WHERE i.term IS NULL OR f.docfreq <> i.docfreq OR f.totalfreq <> i.totalfreq;

SELECT * FROM termlist_full f LEFT JOIN termlist_incr i ON f.term = i.term
WHERE i.term IS NULL OR f.docfreq <> i.docfreq OR f.totalfreq <> i.totalfreq;

SELECT * FROM termlist_full;
SELECT * FROM termlist_incr;

DROP TABLE termlist_full;
DROP TABLE termlist_incr;