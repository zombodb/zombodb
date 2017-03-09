SELECT assert(count(*), 35560, 'syntax-numeric_field_not_null-2') FROM so_posts WHERE zdb(so_posts) ==> 'answer_count!=null';
