{ lib
, naersk
, stdenv
, clangStdenv
, cargo-pgrx
, hostPlatform
, targetPlatform
, postgresql
, postgresql_10
, postgresql_11
, postgresql_12
, postgresql_13
, pkg-config
, openssl
, libiconv
, rustfmt
, cargo
, rustc
, llvmPackages
, pgrxPostgresVersion ? 11
}:

let
  pgrxPostgresPkg =
    if (pgrxPostgresVersion == 10) then postgresql_10
    else if (pgrxPostgresVersion == 11) then postgresql_11
    else if (pgrxPostgresVersion == 12) then postgresql_12
    else if (pgrxPostgresVersion == 13) then postgresql_13
    else null;
  pgrxPostgresVersionString = builtins.toString pgrxPostgresVersion;
  cargoToml = (builtins.fromTOML (builtins.readFile ./Cargo.toml));
in

naersk.lib."${targetPlatform.system}".buildPackage rec {
  name = "${cargoToml.package.name}-pg${pgrxPostgresVersionString}";
  version = cargoToml.package.version;

  src = ./.;

  inputsFrom = [ postgresql_10 postgresql_11 postgresql_12 postgresql_13 cargo-pgrx ];

  LIBCLANG_PATH = "${llvmPackages.libclang}/lib";
  buildInputs = [
    rustfmt
    cargo-pgrx
    pkg-config
    cargo
    rustc
    libiconv
  ];
  checkInputs = [ cargo-pgrx cargo rustc ];
  doCheck = true;

  preConfigure = ''
    mkdir -p $out/.pgrx/{10,11,12,13}
    export PGRX_HOME=$out/.pgrx

    cp -r -L ${postgresql_10}/. $out/.pgrx/10/
    chmod -R ugo+w $out/.pgrx/10
    cp -r -L ${postgresql_10.lib}/lib/. $out/.pgrx/10/lib/
    cp -r -L ${postgresql_11}/. $out/.pgrx/11/
    chmod -R ugo+w $out/.pgrx/11
    cp -r -L ${postgresql_11.lib}/lib/. $out/.pgrx/11/lib/
    cp -r -L ${postgresql_12}/. $out/.pgrx/12/
    chmod -R ugo+w $out/.pgrx/12
    cp -r -L ${postgresql_12.lib}/lib/. $out/.pgrx/12/lib/
    cp -r -L ${postgresql_13}/. $out/.pgrx/13/
    chmod -R ugo+w $out/.pgrx/13
    cp -r -L ${postgresql_13.lib}/lib/. $out/.pgrx/13/lib/

    ${cargo-pgrx}/bin/cargo-pgrx pgrx init \
      --pg10 $out/.pgrx/10/bin/pg_config \
      --pg11 $out/.pgrx/11/bin/pg_config \
      --pg12 $out/.pgrx/12/bin/pg_config \
      --pg13 $out/.pgrx/13/bin/pg_config
    
    # This is primarily for Mac or other Nix systems that don't use the nixbld user.
    export USER=$(whoami)
    export PGDATA=$out/.pgrx/data-${pgrxPostgresVersionString}/
    echo "unix_socket_directories = '$out/.pgrx'" > $PGDATA/postgresql.conf
    ${pgrxPostgresPkg}/bin/pg_ctl start
    ${pgrxPostgresPkg}/bin/createuser -h localhost --superuser --createdb $USER || true
    ${pgrxPostgresPkg}/bin/pg_ctl stop

    # Set C flags for Rust's bindgen program. Unlike ordinary C
    # compilation, bindgen does not invoke $CC directly. Instead it
    # uses LLVM's libclang. To make sure all necessary flags are
    # included we need to look in a few places.
    # TODO: generalize this process for other use-cases.
    export BINDGEN_EXTRA_CLANG_ARGS="$(< ${stdenv.cc}/nix-support/libc-crt1-cflags) \
      $(< ${stdenv.cc}/nix-support/libc-cflags) \
      $(< ${stdenv.cc}/nix-support/cc-cflags) \
      $(< ${stdenv.cc}/nix-support/libcxx-cxxflags) \
      ${lib.optionalString stdenv.cc.isClang "-idirafter ${stdenv.cc.cc}/lib/clang/${lib.getVersion stdenv.cc.cc}/include"} \
      ${lib.optionalString stdenv.cc.isGNU "-isystem ${stdenv.cc.cc}/include/c++/${lib.getVersion stdenv.cc.cc} -isystem ${stdenv.cc.cc}/include/c++/${lib.getVersion stdenv.cc.cc}/${stdenv.hostPlatform.config} -idirafter ${stdenv.cc.cc}/lib/gcc/${stdenv.hostPlatform.config}/${lib.getVersion stdenv.cc.cc}/include"}
    "
  '';
  preCheck = ''
    export PGRX_HOME=$out/.pgrx
    export NIX_PGLIBDIR=$out/.pgrx/${pgrxPostgresVersionString}/lib
  '';
  preBuild = ''
    export PGRX_HOME=$out/.pgrx
    ${cargo-pgrx}/bin/cargo-pgrx pgrx schema
    ls -lah ./sql
    cp -v ./sql/* $out/
    rm -v $out/load-order.txt
    cp -v ./${cargoToml.package.name}.control $out/${cargoToml.package.name}.control
  '';
  preFixup = ''
    rm -r $out/.pgrx
    mv $out/lib/* $out/
    rm -r $out/lib $out/bin
  '';
  PGRX_PG_SYS_SKIP_BINDING_REWRITE = "1";
  CARGO_BUILD_INCREMENTAL = "false";
  RUST_BACKTRACE = "full";
  # This is required to have access to the `sql/*.sql` files.
  singleStep = true;

  cargoBuildOptions = default: default ++ [ "--no-default-features" "--features \"pg${pgrxPostgresVersionString}\"" ];
  cargoTestOptions = default: default ++ [ "--no-default-features" "--features \"pg_test pg${pgrxPostgresVersionString}\"" ];
  copyLibs = true;

  meta = with lib; {
    description = cargoToml.package.description;
    homepage = cargoToml.package.homepage;
    license = with licenses; [ mit ];
    maintainers = with maintainers; [ hoverbear ];
  };
}
