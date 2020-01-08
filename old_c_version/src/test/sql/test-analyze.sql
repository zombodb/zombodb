SELECT * FROM zdb.analyze_text('idxevents', 'standard', 'This is a test, 42 https://www.zombodb.com');
SELECT * FROM zdb.analyze_custom(index=>'idxevents', text=>'This is a test, 42 https://www.zombodb.com', tokenizer=>'keyword', filter=>ARRAY['lowercase']);
SELECT * FROM zdb.analyze_with_field('idxevents', 'event_type', 'This is a test, 42 https://www.zombodb.com/');