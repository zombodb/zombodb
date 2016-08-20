package com.tcdi.zombodb.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.FilteredQueryParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public class ExpansionQueryBuilder extends BaseQueryBuilder {

    private final String fieldname;
    private QueryBuilder query;

    public ExpansionQueryBuilder(String name) {
        fieldname = name;
    }

    public ExpansionQueryBuilder query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ExpansionQueryParser.NAME);
        builder.field("name", fieldname);
        if (query != null) {
            builder.field("query");
            query.toXContent(builder, params);
        }
        builder.endObject();
    }
}
