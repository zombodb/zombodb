SELECT *
FROM zdb_json_aggregate('so_posts', '{
        "top-tags": {
            "terms": {
                "field": "tags",
                "size": 3
            },
            "aggs": {
                "top_tag_hits": {
                    "top_hits": {
                        "sort": [
                            {
                                "last_activity_date": {
                                    "order": "desc"
                                }
                            }
                        ],
                        "size" : 1
                    }
                }
            }
        }
    }', 'java');