-- highlight the entire value of 'Roger Pate' in "user_display_name"
select highlights.* from
    so_comments left join lateral
        zdb.highlight_document('so_comments', to_jsonb(so_comments), 'id:1541 or user_display_name = "Roger Pate"') as highlights on true
where id = 1541 order by field_name;
