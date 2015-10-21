SELECT assert(count(*), 169, 'syntax-term_not_term') FROM so_posts WHERE zdb('so_posts', ctid) ==> 'beer not food';
