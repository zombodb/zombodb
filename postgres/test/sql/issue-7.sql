SELECT assert((rest_get(zdb_get_url('idxso_posts') || zdb_get_index_name('idxso_posts') || '/_settings') ->
               zdb_get_index_name('idxso_posts') -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');

SELECT assert((rest_get(zdb_get_url('idxso_users') || zdb_get_index_name('idxso_users') || '/_settings') ->
               zdb_get_index_name('idxso_users') -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');

SELECT assert((rest_get(zdb_get_url('idxwords') || zdb_get_index_name('idxwords') || '/_settings') ->
               zdb_get_index_name('idxwords') -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');
