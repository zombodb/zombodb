CREATE OR REPLACE FUNCTION zdbtupledeletedtrigger() RETURNS trigger AS '$libdir/plugins/zombodb' language c;
