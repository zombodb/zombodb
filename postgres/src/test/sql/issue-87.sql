SELECT * FROM zdb_highlight('products', 'long_description="but"', 'id IN (1,2,3,4,5)');  -- works
SELECT * FROM zdb_highlight('products', 'long_description="(but"', 'id IN (1,2,3,4,5)');  -- errors
SELECT * FROM zdb_highlight('products', 'long_description="(but)"', 'id IN (1,2,3,4,5)');  -- errors
SELECT * FROM zdb_highlight('products', 'long_description="#but"', 'id IN (1,2,3,4,5)');  -- errors
SELECT * FROM zdb_highlight('products', 'long_description="@but"', 'id IN (1,2,3,4,5)');  -- errors