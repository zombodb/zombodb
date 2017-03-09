select * from zdb_tally('so_posts', 'owner_display_name', '^.*', '(  #expand<parent_id=<this.index>id> (title:java) ) ', 1000, 'term');
select * from zdb_tally('so_posts', 'owner_display_name', '^.*', '(  #expand<parent_id=<this.index>id> (title:java) ) ', 1000, 'term');
select id, owner_display_name from so_posts where zdb(so_posts) ==> '#expand<owner_display_name=<this.index>owner_display_name>(id:39338)' order by 1;
select id, owner_display_name from so_posts where zdb(so_posts) ==> '#expand<owner_display_name=<this.index>owner_display_name>(beer)' order by 1;
select id, owner_display_name from so_posts where zdb(so_posts) ==> '#expand<owner_display_name=<this.index>owner_display_name>(beer or owner_display_name:s*)' order by 1;
select id, owner_display_name from so_posts where zdb(so_posts) ==> 'body:(beer w/500 a)' order by 1;
select id, owner_display_name from so_posts where zdb(so_posts) ==> '#expand<parent_id=<this.index>parent_id>((beer w/500 a))' order by 1;
select id, owner_display_name from so_posts where zdb(so_posts) ==> '#expand<parent_id=<this.index>parent_id>((beer w/500 a))' order by 1;
select assert(
    (select count(*) from so_posts where zdb(so_posts) ==> '#expand<owner_display_name=<this.index>owner_display_name>(beer or #expand<parent_id=<this.index>parent_id>(owner_display_name:s*))'),
    (select count(*) from so_posts where zdb(so_posts) ==> '#expand<owner_display_name=<this.index>owner_display_name>(#expand<parent_id=<this.index>parent_id>(owner_display_name:s*) or beer)'),
    'same'
);

