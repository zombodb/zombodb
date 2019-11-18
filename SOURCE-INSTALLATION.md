# ZomboDB Source Installation Instructions

If you're a [sponsor](https://github.com/sponsors/eeeebbbbrrrr) you might want to also read the [BINARY-INSTALLATION.md] documentation.

## Prerequisites

- Postgres 10.x or 11.x installed, including the "-server" packages, or Postgres installed from sources
- `pg_config` in your `$PATH`
- libcurl-devel/libcurl-dev >= 7.28.0 installed
- zlib-devel/libz-dev
- A 64bit Intel Architecture
- Optional:  A working Docker installation

## Building from Sources

First, `pg_config` must be in your `$PATH` and be the binary for your target Postgres installation.

You also need the **development** packages for `libcurl` and `libz`.  The package names vary between various Linux distributions
so make sure to check your distro's documentation to determine which to install.

Secondly, clone ZomboDB's GitHub repo (https://github.com/zombodb/zombodb.git), change into the created directory, and
for all UNIX-based platforms (including MacOS) simply run:

```shell
$ make clean install
```

This will compile ZomboDB **and** install it into the Postgres installation described by `pg_config`.  The user that
runs `make clean install` will need write permissions to the Postgres `$PG_INSTALL_PATH/lib/postgresql/` and `$PG_INSTALL_PATH/share/postgresql/extension/` directories

After that you can test ZomboDB by running its regression test suite:

```sqll
$ make installcheck-setup installcheck
```

Note that in order for the regression test system to find your Elasticsearch server, you need to add this to your
`postgresql.conf` file (and restart Postgres):

```
zdb.default_elasticsearch_url = 'http://localhost:9200/'
```

... where the url points to your Elasticsearch server.


## Updating ZomboDB to a New Version

Updating ZomboDB from sources will simply require a `git pull`, another `make clean install` and running 
`ALTER EXTENSION zombodb UPDATE;` in all databases that use the ZomboDB extension.


## Building binary artifacts with Docker

If you have a proper Docker installation you can simply run:

```shell script
$ make release
```

This process will take awhile the first time (potentially hours depending on your download speeds), but it will build
ZomboDB for all supported Linux distro and Postgres version permutations.

The final binary artifacts will be placed in the `target/` directory, along with logs should there be build failures.

Then you can basically follow the instructions in [BINARY-INSTALLATION.md] to install the proper artifact for your
environment.