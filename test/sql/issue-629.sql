select count(*)
from so_posts
where so_posts ==>
      '{ "function_score": { "query": { "match_all": {} }, "field_value_factor": { "field": "answer_countd" } } }';