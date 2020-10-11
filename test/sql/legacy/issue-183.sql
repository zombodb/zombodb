--
-- should return just id=2,3,4
--
SELECT id FROM products WHERE products ==> 'id<>1' order by id;
SELECT id FROM products WHERE products ==> 'id<>[1]' order by id;
SELECT id FROM products WHERE products ==> 'id<>[[1]]' order by id;

--
-- should return all 4 rows
--
SELECT id FROM products WHERE id <> 1 OR id <> 2 OR id <> 3 ORDER BY id;
SELECT id FROM products WHERE products ==> 'id<>1 OR id<>2 OR id <> 3' ORDER BY id;

--
-- should return just id=4
--
SELECT id FROM products WHERE id <> 1 AND id <> 2 AND id <> 3;
SELECT id FROM products WHERE products ==> 'id<>1 AND id<>2 AND id <>3';
SELECT id FROM products WHERE products ==> 'id<>[1,2,3]';
SELECT id FROM products WHERE products ==> 'id<>[[1,2,3]]';

