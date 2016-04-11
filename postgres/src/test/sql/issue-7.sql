SELECT assert((rest_get(zdb_get_url('idxso_posts') || zdb_get_index_name('idxso_posts') || '.0/_settings') ->
               (zdb_get_index_name('idxso_posts')||'.0') -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');

SELECT assert((rest_get(zdb_get_url('idxso_users') || zdb_get_index_name('idxso_users') || '.0/_settings') ->
               (zdb_get_index_name('idxso_users')||'.0') -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');

SELECT assert((rest_get(zdb_get_url('idxwords') || zdb_get_index_name('idxwords') || '.0/_settings') ->
               (zdb_get_index_name('idxwords')||'.0') -> 'settings' -> 'index' ->> 'number_of_replicas')::bigint, 1,
              'Default replica count of 1');
