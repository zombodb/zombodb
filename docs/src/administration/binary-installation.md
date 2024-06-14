# ZomboDB Installation Instructions

ZomboDB is a 100% native Postgres extension.  Additionally, ZomboDB is available as binary `.deb` and `.rpm` packages to
[sponsors](https://github.com/sponsors/eeeebbbbrrrr) for popular Linux distributions.  As such, installation is fairly straightforward.

If you instead prefer to install ZomboDB from source, please read the [SOURCE-INSTALLATION.md] documentation.


## Prerequisites

- Postgres 10.x, 11.x, 12.x, 13.x installed
- A 64bit Intel Architecture

### Installation on CentOS/RHEL

Download the proper `.rpm` package for your CentOS/RHEL distro from https://www.zombodb.com/${your_download_key}/, and simply run:

```shell
$ rpm -Uvh zombodb_centos-8_pg10-3000.0.0-alpha1_1.x86_64.rpm
```

### Installation on Ubuntu/Debian

Download the proper `.deb` package for your Ubuntu/Debian distro from https://www.zombodb.com/${your_download_key}/, and simply run:

```shel
$ dpkg -i zombodb_ubuntu-focal_pg10-3000.0.0-alpha1_amd64.deb
```

### Installation on MacOS

Please see the [source installation documentation](SOURCE-INSTALLATION.md).


## `postgresql.conf` Settings to Consider

ZomboDB defaults to zero Elasticsearch index replicas.  If you're installing for a production system you might consider setting `zdb.default_replicas` to a better value.

You might also consider setting `zdb.default_elasticsearch_url`.

Both of these values can be set per index, so they're not strictly necessary to set in `postgresql.conf`.

Make sure to read about ZomboDB's [configuration settings](CONFIGURATION-SETTINGS.md) and its [index options](INDEX-MANAGEMENT.md#with--options).

## Verifying Installation

Once installed, ensure Postgres is running (it does **not** need to be restarted).  Then you can create a test database and create the ZomboDB extension.

Note that a Postgres "superuser" must issue the `CREATE EXTENSION zombodb;` statement.


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

Keep in mind that ZomboDB requires Elasticsearch 7.x.

Detailed advice about managing and configuring Elasticsearch clusters is outside the scope of this document, however commercial support can be purchased from ZomboDB, LLC.  Feel free to contact us via https://www.zombodb.com/services.

That said, ZomboDB has been tested against various cloud-hosted Elasticsearch providers such as Bonsai (https://bonsai.io) and Elastic's own [Elasticsearch Service](https://www.elastic.co/cloud/elasticsearch-service).  ZomboDB can also be used with your own internally-managed Elasticsearch clusters.

