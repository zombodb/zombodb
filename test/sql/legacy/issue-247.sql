set datestyle to 'iso, mdy';

create table issue247 (
  id serial8,
  date_field date,
  timestamp_field timestamp
);

create index idxissue247 on issue247 using zombodb ( (issue247.*) );

insert into issue247 (date_field, timestamp_field) values ('March 20, 1977', 'March 20, 1977 3:49pm PDT');
insert into issue247 (date_field, timestamp_field) values ('March 20, 1977', 'March 20, 1977 3:49pm PDT');
insert into issue247 (date_field, timestamp_field) values ('March 20, 1977', 'March 20, 1977 3:49pm PDT');

SELECT term::date, count FROM zdb.tally('idxissue247', 'date_field', '0', '^.*', '', 2147483647,'term');
SELECT term::timestamp, count FROM zdb.tally('idxissue247', 'timestamp_field', '0', '^.*', '', 2147483647,'term');

SELECT term, count FROM zdb.tally('idxissue247', 'date_field', '0', 'day', '', 2147483647,'term');
SELECT term, count FROM zdb.tally('idxissue247', 'date_field', '0', 'month', '', 2147483647,'term');
SELECT term, count FROM zdb.tally('idxissue247', 'date_field', '0', 'year', '', 2147483647,'term');

SELECT term, count FROM zdb.tally('idxissue247', 'timestamp_field', '0', 'day', '', 2147483647,'term');
SELECT term, count FROM zdb.tally('idxissue247', 'timestamp_field', '0', 'month', '', 2147483647,'term');
SELECT term, count FROM zdb.tally('idxissue247', 'timestamp_field', '0', 'year', '', 2147483647,'term');

drop table issue247 cascade;


