# ZomboDB Installation

ZomboDB consists of two pieces.  One is a Postgres Extension (written in C and SQL/PLPGSQL), and the other is an Elasticsearch plugin (written in Java).

Currently ZomboDB only supports Postgres `v9.5` (on Linux) and Elasticsearch `v1.7+` (not `v2.x`)


## Postgres Extension

[Download](https://github.com/zombodb/zombodb/releases/latest) the `.rpm` or `.deb` package and execute the command approporiate for your Linux distribution:

RHEL/CentOS:

```
# sudo rpm -Uvh zombodb-X.Y.Z-1.x86_64.rpm
```

Ubuntu:

```
# sudo dpkg -i zombodb-Z.Y.Z_amd64.deb
```

**Note:**  The Postgres extension installs a few commit and executor hooks. As such it needs to be configured as a preloaded local library.  Do this by editing  `postgresql.conf` to add the following:

```
local_preload_libraries = 'zombodb.so'
```

> Failure to add `zombodb.so` to `local_preload_libraries` will cause unexpected things in a database that has the extension installed!)

After the above is done, restart Postgres.  Then you can create the extension in a new/existing database:

```
$ psql example -c "CREATE EXTENSION zombodb;"
```

## Elasticsearch Plugin

ZomboDB's Elasticsearch plugin needs to be installed on all nodes of your Elasticsearch cluster.  This became a requirement as of v2.6.7.

[Download](https://github.com/zombodb/zombodb/releases/latest) the latest release `.zip` file and use Elasticsearch's plugin utility to install ZomboDB:

```
# cd $ES_HOME
# sudo bin/plugin -i zombodb -u file:///path/to/zombodb-plugin-X.X.X.zip
```

There are a few configuration settings that **must** be set in `elasticsearch.yml`:

```
script.disable_dynamic: false

threadpool.bulk.queue_size: 1024
threadpool.bulk.size: 12

http.max_content_length: 1024mb
index.query.bool.max_clause_count: 1000000
```

Dynamic scripting must **not** be disabled.  The bulk threadpool must be increased because ZomboDB multiplexes against the `_bulk` endpoint.

The last two settings can be turned up or down (`http.max_content_length` must be be greater than 8192kB), but are good defaults.

Finally, restart the node.  Repeat for every "client" node in your cluster.


# Upgrading

Upgrading to a new version of ZomboDB basically involves repeating the installation steps above, possibly first removing the existing `zombodb` Linux package.

When upgrading the Postgres extension (`zombodb.so`) it's a good idea to make sure the database has no active connections and that you immediately run:

```sql
ALTER EXTENSION zombodb UPDATE;
```

in every database that contains the extension.

The existing Elasticsearch plugin will need to be removed (`bin/plugin -r zombodb`) before an updated version can be installed.

