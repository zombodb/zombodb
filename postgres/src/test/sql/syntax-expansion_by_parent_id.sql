SELECT assert(count(*), 12, 'syntax-expansion_by_parent_id') FROM so_posts WHERE zdb(so_posts) ==> '#expand<parent_id=<this.index>parent_id>(id:857)';
