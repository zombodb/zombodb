create table ft
(
    ft   zdb.fulltext,
    ftws zdb.fulltext_with_shingles
);
insert into ft (ft, ftws)
values ('The map described sailing 23° North by 17° East to reach the island.
The name of the island is ⅋☺︎＠¡¿ To retrieve the treasure, find the largest
tree behind BestBuy® and click the treasure chest icon.',
        'The map described sailing 23° North by 17° East to reach the island.
The name of the island is ⅋☺︎＠¡¿ To retrieve the treasure, find the largest
tree behind BestBuy® and click the treasure chest icon.');

create index idxft on ft using zombodb ((ft.*));

select *
from zdb.highlight_document('ft', (select to_json(ft.*) from ft),
    'ft:treasure or ftws:treasure')
order by start_offset, field_name;

drop table ft;