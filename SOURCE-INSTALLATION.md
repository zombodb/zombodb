# ZomboDB Source Installation Instructions

If you're a [sponsor](https://github.com/sponsors/eeeebbbbrrrr) you might want to also read the
\[BINARY-INSTALLATION.md\] documentation.

## Prerequisites

- Postgres 10.x, 11.x, 12.x, or 13.x installed (probably from your package manager), including the "-server" package

- A toolchain capable of building Postgres:

  For Ubuntu, this is enough:

  ```
  apt install bison flex zlib1g zlib1g-dev \
      pkg-config make libssl-dev libreadline-dev
  ```

- The Rust toolchain

- `cargo install cargo-pgrx`

- A 64bit Intel Architecture

## Building from Sources

First off, and you'll only need to do this once, you need to initialize `cargo-pgrx`, and you'll want to tell it the path
to the version of Postgres on your computer.

If that version of Postgres is pg12, and assuming its `pg_config` tool is on your `$PATH`, this will likely work:

```shell script
$ cargo pgrx init --pg12=`which pg_config`
```

Next, clone this repo, change into the checkout directory and simply run:

```shell script
$ cargo pgrx install --release
```

This will compile ZomboDB **and** install it into the Postgres installation described by `pg_config`. The user that runs
the above command will need write permissions to the Postgres `$PG_INSTALL_PATH/lib/postgresql/` and
`$PG_INSTALL_PATH/share/postgresql/extension/` directories.

## Updating ZomboDB to a New Version

Updating ZomboDB from sources will simply require a `git pull`, another `make clean install` and running
`ALTER EXTENSION zombodb UPDATE;` in all databases that use the ZomboDB extension.

## TLS Support via `rustls`
ZomboDB supports TLS connections to Elasticsearch via the `rustls` and `native-tls` crates. By default, ZomboDB is compiled with `rustls` support. By default it will trust [webpki-roots](https://docs.rs/webpki-roots/latest/webpki_roots/), a copy of the Mozilla Root program that is bundled into ZomboDB.

If you want to use custom certificates you will need to enable `rustls_native_certs` feature when building ZomboDB. This will use the OS certificate verifier and trust store. 

> [!NOTE]  
> If you want to load a certificate from the file - you can set the `SSL_CERT_FILE` environment variable to the path of the certificate file.


## Enabling TLS Support via `native-tls`
By default, ZomboDB is compiled with `rustls` support.  If you'd like to use `native-tls` instead, you'll need to enable the `native_tls` feature when building ZomboDB.

Both `rustls` and `native-tls` support TLS connections via the `https` scheme. Both features will use the OS certificate verifier and trust store.

You may want to use `native-tls` if you are using TLS 1.1 or 1.0, as `rustls` does not support these older TLS versions or if you need to validate the certificates for IP addresses.

## Building Binary Artifacts with Docker

If you have a proper Docker installation you can simply run:

```shell script
$ cd docker-build-system
$ cargo run <branch-name> [<docker-image-name> <pg major version>]
```

Likely for the `<branch-name>` argument you'll want to specify `master`, unless perhaps you're working on a custom
branch.

If you're only targeting one Linux distro and Postgres version, you'll want to specify all three arguments.

This process will take a long time (potentially hours depending on your download speeds), but it will build ZomboDB for
all supported Linux distro and Postgres version permutations.

You can set an environment variable named `CPUS` to limit the number of CPUs the build process will use, but the default
is however many your computer has.

The final binary artifacts will be placed in the `./target/zdb-build/artifacts/` directory.

No logs are created, but in the event of Docker/compilation errors, the entire output of the thing that failed is
printed to stdout.

Once binary artifacts are build, follow the instructions in \[BINARY-INSTALLATION.md\] to install the proper artifact
for your environment.
