# ZomboDB Installation Instructions

ZomboDB is a 100% native Postgres extension.  Additionally, ZomboDB is released as binary `.deb` and `.rpm` packages for popular Linux distributions.  As such, installation is fairly straightforward.


## Prerequisites

- Postgres 10.x installed
- libcurl >= 7.28.0 installed
- A 64bit Intel Architecture

### Installation on CentOS/RHEL

Download the proper `.rpm` package for your CentOS/RHEL distro from https://www.zombodb.com/releases, and simply run:

```shell
$ rpm -Uvh zombodb_centos_7_pg10-1.0.0_x86_64.rpm
```

### Installation on Ubuntu/Debian

Download the proper `.deb` package for your Ubuntu/RHEL distro from https://www.zombodb.com/releases, and simply run:

```shel
$ dpkg -i zombodb_ubuntu_xenial_pg10-1.0.0_amd64.deb
```

### Installation on MacOS

For MacOS, ZomboDB can only be installed from source.  Detailed compilation instructions are outside the scope of this document, but in general simply cloning the ZomboDB repository, switching to the `master` branch and running:

```shell
$ make clean install
```

should do the trick.  You'll need to make sure that Postgres' `pg_config` command-line utility is in your path and that you have the extension development headers (known as PGXS) installed as well.  

## `postgresql.conf` settings to consider

ZomboDB defaults to zero Elasticsearch index replicas.  If you're installing for a production system you might consider setting `zdb.default_replicas` to a better value.

You might also consider setting `zdb.default_elasticsearch_url`.

Both of these values can be set per index, so they're not strictly necessary to set in `postgresql.conf`.

Make sure to read about ZomboDB's [configuration settings](CONFIGURATION-SETTINGS.md) and its [index options](INDEX-MANAGEMENT.md#with--options).

## Verifying Installation

Once installed, ensure Postgres is running (it does **not** need to be restarted).  Then you can create a test database and create the ZomboDB extension.

```shell
$ createdb zdb_test
$ psql zdb_test
psql (10.1)
Type "help" for help.

zdb_test=# CREATE EXTENSION zombodb;
CREATE EXTENSION
```

## Upgrading ZomboDB

When a new ZomboDB version is released and you need to upgrade you should first ensure you have exclusive access to all databases that use ZomboDB -- in other words, make sure there are no active connections.

Once confirmed, you can simply install the new `.deb` or `.rpm` package then for each database that has the ZomboDB extension installed, simply run:

```sql
ALTER EXTENSION zombodb UPDATE;
```

There will be no need to restart Postgres.

While it is unlikely, should a ZomboDB version upgrade require that indices be `REINDEX`ed, that will be noted in the release notes for that version.

## Elasticsearch Considerations

Keep in mind that ZomboDB requires Elasticsearch 5.6+ or 6.x.

Detailed advice about managing and configuring Elasticsearch clusters is outside the scope of this document, however commerical support can be purached from ZomboDB, LLC.  Feel free to contact us via https://www.zombodb.com/services.

That said, ZomboDB has been tested against various cloud-hosted Elasticsearch providers such as Bonsai (https://bonsai.io) and Elastic's own [Elasticsearch Service](https://www.elastic.co/cloud/elasticsearch-service).  ZomboDB can also be used with your own internally-managed Elasticsearch clusters.

