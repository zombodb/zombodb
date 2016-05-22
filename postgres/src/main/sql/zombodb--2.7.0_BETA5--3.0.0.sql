DROP OPERATOR ==>(json, text);

DROP FUNCTION zdbgetbitmap(internal, internal);
UPDATE pg_am SET amgetbitmap = '-' WHERE amname = 'zombodb';