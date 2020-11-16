set -eux

mkdir -p postgresql/pg$1/build

tar xvf /tmp/postgresql-$1.tar.gz --strip-components 1 --directory postgresql/pg$1

cd postgresql/pg$1

./configure --prefix=$(pwd)/build/

make -j "$(nproc)" install
