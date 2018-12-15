SELECT dsl.sort('field', 'asc', dsl.match_all());
SELECT dsl.sort_many(dsl.match_all(), dsl.sd('field', 'asc'), dsl.sd('field2', 'desc'), dsl.sd('field3', 'asc', 'avg'));
SELECT dsl.sort_many(dsl.match_all(), dsl.sd_nested('nested.field', 'asc', 'nested', dsl.match_all(), 'min'), dsl.sd('field2', 'desc'), dsl.sd('field3', 'asc', 'avg'));
SELECT dsl.sort_direct('{
        "_script" : {
            "type" : "number",
            "script" : {
                "lang": "painless",
                "source": "doc[''field_name''].value * params.factor",
                "params" : {
                    "factor" : 1.1
                }
            },
            "order" : "asc"
        }
    }', dsl.match_all());