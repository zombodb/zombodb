select zdb.dump_query('events', 'title:*****');
select zdb.dump_query('events', 'title:"zombodb * awesome"');
select zdb.dump_query('events', 'title:"zombodb * * * * * awesome"');
select zdb.dump_query('events', 'title:"zombodb * * * * ****** awesome"');
select zdb.dump_query('events', 'title:"zombodb \* awesome"');
