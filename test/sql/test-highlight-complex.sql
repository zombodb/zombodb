select *
from zdb.highlight_document('so_comments',
                            '{"comment_text": { "first": ["eric", "david"], "last": "ridge", "id":{"low":0, "high":8}}}'::jsonb,
                            'comment_text.first = "david", comment_text.id.low = 0')
order by field_name, array_index, start_offset;


select *
from zdb.highlight_document('so_comments',
                            '{"comment_text": { "first": ["eric ridge", "david"], "last": "ridge", "id":{"low":0, "high":42}}}'::jsonb,
                            'comment_text.first = "eric r*", comment_text.id.high = 42')
order by field_name, array_index, start_offset;

select *
from zdb.highlight_document('so_comments',
                            '{"comment_text": { "first": ["eric ridge", "david"], "last": "ridge", "id":{"low":0, "high":42}}}'::jsonb,
                            'comment_text.first = "eric r*" and comment_text.first:"joe", comment_text.id.high = 42')
order by field_name, array_index, start_offset;

select *
from zdb.highlight_document('so_comments',
                            '{"comment_text": { "first": ["eric ridge", "david"], "last": "ridge", "id":{"low":0, "high":42}}}'::jsonb,
                            'comment_text.first = "eric r*" and comment_text.first:"david", comment_text.id.high = 42')
order by field_name, array_index, start_offset;

select *
from zdb.highlight_document('so_comments',
                            '{"comment_text": { "first": ["eric ridge", "david"], "last": "ridge", "id":{"low":0, "high":42}}}'::jsonb,
                            'comment_text.first = "eric r*" with comment_text.first:"david", comment_text.id.high = 42')
order by field_name, array_index, start_offset;


