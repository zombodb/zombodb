SELECT assert((zdb_es_direct_request('idxso_posts', 'GET', '_settings')::json ->
               (zdb_get_index_name('idxso_posts')) -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');

SELECT assert((zdb_es_direct_request('idxso_users', 'GET', '_settings')::json ->
               (zdb_get_index_name('idxso_users')) -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');

SELECT assert((zdb_es_direct_request('idxwords', 'GET', '_settings')::json ->
               (zdb_get_index_name('idxwords')) -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');
