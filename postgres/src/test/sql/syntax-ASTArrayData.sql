SELECT assert(count(*), 17, 'syntax-ASTArrayData') FROM so_posts WHERE zdb('so_posts', ctid) ==> 'id = [[1,3,4,7,9,12,13,16,18,20,22,23,25,26,38,39,41]]';
