SELECT assert((zdb.request('idxso_posts', '_settings', 'GET' )::json ->
               (zdb.index_name('idxso_posts')) -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 0,
              'Default replica count of 0');

SELECT assert((zdb.request('idxso_users', '_settings', 'GET')::json ->
               (zdb.index_name('idxso_users')) -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 0,
              'Default replica count of 0');

SELECT assert((zdb.request('idxwords', '_settings', 'GET')::json ->
               (zdb.index_name('idxwords')) -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 0,
              'Default replica count of 0');
