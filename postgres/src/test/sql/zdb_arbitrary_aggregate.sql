SELECT *
FROM zdb_arbitrary_aggregate('so_posts',
                             '#tally(tags, ''^.*'', 25, ''term'', #tally(owner_display_name, ''^.*'', 150, ''term''))',
                             '');