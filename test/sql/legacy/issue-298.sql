SELECT id
FROM so_posts
WHERE zdb('so_posts', ctid) ==>
      '#subselect<id=<so_comments.idxso_comments>post_id>(id:4694)
            AND
       #subselect<id=<so_comments.idxso_comments>post_id>(id:9422)';