select *
from zdb.highlight_document('idxso_posts', '
{
  "my_object_array": [
    {
      "state": "foo"
    },
    {
      "state": "bar"
    },
    {
      "state": "baz"
    }
  ]
}
', 'my_object_array.state = [bar, foo, baz]') order by 1, 2;