SELECT count(*) FROM words;

SELECT count(*) FROM (SELECT words.word, assert(count(*), 1, words.word) FROM words INNER JOIN (SELECT word FROM words ORDER BY random() limit 2500) list ON words.word = list.word WHERE zdb(words) ==> ('word:"' || list.word || '"')::text GROUP BY words.word) x;

SELECT zdb_estimate_count('words', '') = (SELECT count(*) FROM words);
