#! /bin/sh
PGPORT=65432 PGDATA=/zdb/pg10/data PATH=/zdb/pg10/bin:$PATH make clean install installcheck-setup installcheck && \
PGPORT=8543  PGDATA=/zdb/pg11/data PATH=/zdb/pg11/bin:$PATH make clean install installcheck-setup installcheck && \
make clean install


