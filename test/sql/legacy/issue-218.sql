-- SQL
SELECT id FROM products WHERE (id=1 OR id=2 OR id=3) ORDER BY id;		-- 1, 2, 3 (correct)
-- zdb
SELECT id FROM products WHERE products ==> '(id=1 OR id=2 OR id=3)' ORDER BY id;	-- 1, 2, 3 (correct)
SELECT id FROM products WHERE products ==> '(id=1,id=2,id=3)' ORDER BY id;		-- 1, 2, 3 (correct)


-- SQL
SELECT id FROM products WHERE (id=1 AND id=2 AND id=3) ORDER BY id;		        -- empty result set (correct)
-- zdb
SELECT id FROM products WHERE products ==> '(id=1 AND id=2 AND id=3)' ORDER BY id;  	-- 1, 2, 3 (wrong)
SELECT id FROM products WHERE products ==> '(id=1 & id=2 & id=3)' ORDER BY id;		-- 1, 2, 3 (wrong)