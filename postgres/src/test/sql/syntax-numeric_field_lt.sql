SELECT assert(count(*), 35256, 'syntax-numeric_field_lt') FROM so_posts WHERE zdb(so_posts) ==> 'answer_count<20';
