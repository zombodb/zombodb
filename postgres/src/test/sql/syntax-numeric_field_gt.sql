SELECT assert(count(*), 256, 'syntax-numeric_field_gt') FROM so_posts WHERE zdb(so_posts) ==> 'answer_count>20';
