SELECT assert(count(*), 129680, 'syntax-numeric_field_null') FROM so_posts WHERE zdb('so_posts', ctid) ==> 'answer_count=null';
