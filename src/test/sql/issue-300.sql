select e.id, event_type, zdb.score(u.ctid) <> 0 from events e, users u where e.user_id = u.id and event_type = 'IssueCommentEvent' and u ==> 'vicjoecs' order by zdb.score(u.ctid) desc, id;

select sum(zdb.score(ctid)) > 0 from events where events ==> 'beer';
select sum(zdb.score(ctid)) > 0, event_type from events where events ==> 'beer' group by event_type order by event_type;