CREATE TABLE issue159 (pk_id bigint not null primary key, groupid text, has_att char);
CREATE INDEX idxissue159 ON issue159 USING zombodb (zdb('issue159', ctid), zdb(issue159)) WITH (url='http://localhost:9200/');

INSERT INTO issue159 VALUES (239558, '0000003158/3492', 'A');
INSERT INTO issue159 VALUES (239557, '0000003158/3492', 'Y');
SELECT pk_id, groupid, has_att
FROM issue159
WHERE zdb('issue159', ctid) ==> '( (#expand<groupid=<this.index>groupid>( ( pk_id = 239558) )) )'
ORDER BY pk_id;


SELECT pk_id, groupid, has_att
FROM issue159
WHERE zdb('issue159', ctid) ==> '( (#expand<groupid=<this.index>groupid>( ( pk_id = 239558) #filter(has_att = A) )) )'
ORDER BY pk_id;


SELECT pk_id, groupid, has_att
FROM issue159
WHERE zdb('issue159', ctid) ==> '( (#expand<groupid=<this.index>groupid>( ( pk_id = 239558) #filter(has_att = Y) )) )'
ORDER BY pk_id;

DROP TABLE issue159;
