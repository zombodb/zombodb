create schema wi60341;

create table wi60341.hili_recreate (
                                       pk_data_id serial8,
                                       data_full_text zdb.fulltext_with_shingles,
                                       constraint idx_wi60341_hili_recreate_pkey primary key (pk_data_id));

create index es_wi60341_hili_recreate on wi60341.hili_recreate using zombodb ((wi60341.hili_recreate.*)) with (shards='5', replicas='1', max_analyze_token_count='10000000', max_terms_count='2147483647');


insert into wi60341.hili_recreate (pk_data_id, data_full_text) values (1, E'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\nLorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\nLorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\nLorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\n');

-- first query
select highlights.*
from (
         select *
         from wi60341.hili_recreate
         where pk_data_id = 1
     ) crv
         inner join lateral
    zdb.highlight_document('wi60341.hili_recreate'::regclass,to_json(crv),
                           'data_full_text:("Duis*" W/3 "aute*")'::text) as highlights ON true
order by position, start_offset, end_offset, term;

drop schema wi60341 cascade;