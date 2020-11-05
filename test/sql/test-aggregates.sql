SELECT * FROM zdb.terms('idxevents', 'event_type', dsl.match_all());
SELECT * FROM zdb.count('idxevents', dsl.match_all());
SELECT * FROM zdb.stats('idxevents', 'repo_id', dsl.match_all());