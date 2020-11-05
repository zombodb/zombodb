# ZomboDB Source Installation Instructions

If you're a [sponsor](https://github.com/sponsors/eeeebbbbrrrr) you might want to also read the [BINARY-INSTALLATION.md] documentation.

## Prerequisites

- Postgres 10.x, 11.x, or 12.x installed (probably from your package manager), including the "-server" package
- A toolchain capable of building Postgres:

    For Ubuntu, this is enough:
    
    ```
    apt install bison flex zlib1g zlib1g-dev \
        pkg-config libssl-dev libreadline-dev
    ```

- The Rust toolchain
- `cargo install cargo-pgx`
- A 64bit Intel Architecture

## Building from Sources

First off, and you'll only need to do this once, you need to initialize `cargo-pgx`, and
you'll want to tell it the path to the version of Postgres on your computer.

If that version of Postgres is pg12, and assuming its `pg_config` tool is on your `$PATH`, 
this will likely work:

```shell script
$ cargo pgx init --pg12=`which pg_config`
```

Next, clone this repo, change into the checkout directory and simply run:

```shell script
$ cargo pgx install --release
```

This will compile ZomboDB **and** install it into the Postgres installation described by `pg_config`.  The user that
runs the above command will need write permissions to the Postgres `$PG_INSTALL_PATH/lib/postgresql/` and `$PG_INSTALL_PATH/share/postgresql/extension/` directories

## Updating ZomboDB to a New Version

Updating ZomboDB from sources will simply require a `git pull`, another `make clean install` and running 
`ALTER EXTENSION zombodb UPDATE;` in all databases that use the ZomboDB extension.


## Building binary artifacts with Docker

If you have a proper Docker installation you can simply run:

```shell script
$ cd docker-build-system
$ CPUS=4 ./build.sh master
```

This process will take a long time (potentially hours depending on your download speeds), but it will build
ZomboDB for all supported Linux distro and Postgres version permutations.

If you don't set the `CPUS` environment variable, the build script will default to 64.  If you don't have 64 CPUs, this
probably not what you want.

The final binary artifacts will be placed in the `./target/artifacts/` directory.
Logs for the build process, should there be failures, will be in `./target/logs/`

Then you can basically follow the instructions in [BINARY-INSTALLATION.md] to install the proper artifact for your
environment.