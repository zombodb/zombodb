SELECT assert(count(*), 7626, 'syntax-numeric_field_exact-2') FROM so_posts WHERE zdb(so_posts) ==> 'answer_count=2';
