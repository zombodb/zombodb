SELECT assert(count(*), 52, 'syntax-numeric_field_range') FROM so_posts WHERE zdb(so_posts) ==> 'id:1 /to/ 100';
