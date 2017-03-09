SELECT assert(count(*), 40, 'syntax-phrase_with_wildcard') FROM so_posts WHERE zdb(so_posts) ==> '"function overload*"';
