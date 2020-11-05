select zdb.highlight(ctid, 'payload.commits.message') from events where events ==> 'payload.commits.message:*beer*' order by id limit 10;

set enable_indexscan to off;
set enable_bitmapscan to off;
select zdb.highlight(ctid, 'payload.commits.message') from events where events ==> 'payload.commits.message:*beer*' order by id limit 10;