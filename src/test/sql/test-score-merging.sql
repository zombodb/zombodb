-- this once caused a segfault during development
SELECT zdb.score(ctid) > 1.0 FROM events WHERE events ==> 'beer' OR events ==> 'wine';