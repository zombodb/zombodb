set datestyle to 'iso, mdy';

create table issue247 (
  id serial8,
  date_field date,
  timestamp_field timestamp
);

create index idxissue247 on issue247 using zombodb (zdb('issue247', ctid), zdb(issue247)) with (url='localhost:9200/');

insert into issue247 (date_field, timestamp_field) values ('March 20, 1977', 'March 20, 1977 3:49pm PDT');
insert into issue247 (date_field, timestamp_field) values ('March 20, 1977', 'March 20, 1977 3:49pm PDT');
insert into issue247 (date_field, timestamp_field) values ('March 20, 1977', 'March 20, 1977 3:49pm PDT');

SELECT term, count FROM zdb_tally('issue247', 'date_field', '0', '^.*', '', 2147483647,'term'::zdb_tally_order);
SELECT term, count FROM zdb_tally('issue247', 'timestamp_field', '0', '^.*', '', 2147483647,'term'::zdb_tally_order);

SELECT term, count FROM zdb_tally('issue247', 'date_field', '0', 'day', '', 2147483647,'term'::zdb_tally_order);
SELECT term, count FROM zdb_tally('issue247', 'date_field', '0', 'month', '', 2147483647,'term'::zdb_tally_order);
SELECT term, count FROM zdb_tally('issue247', 'date_field', '0', 'year', '', 2147483647,'term'::zdb_tally_order);

SELECT term, count FROM zdb_tally('issue247', 'timestamp_field', '0', 'day', '', 2147483647,'term'::zdb_tally_order);
SELECT term, count FROM zdb_tally('issue247', 'timestamp_field', '0', 'month', '', 2147483647,'term'::zdb_tally_order);
SELECT term, count FROM zdb_tally('issue247', 'timestamp_field', '0', 'year', '', 2147483647,'term'::zdb_tally_order);

drop table issue247 cascade;


