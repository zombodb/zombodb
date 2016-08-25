/*
 * Copyright 2016 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tcdi.zombodb.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public class ZomboDBVisibilityQueryBuilder extends BaseQueryBuilder {

    private final String fieldname;
    private boolean haveXmin;
    private long xmin;
    private boolean haveXmax;
    private long xmax;
    private long[] activeXids;

    private QueryBuilder query;

    public ZomboDBVisibilityQueryBuilder(String name) {
        fieldname = name;
    }

    public ZomboDBVisibilityQueryBuilder xmin(long xmin) {
        this.xmin = xmin;
        haveXmin = true;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder xmax(long xmax) {
        this.xmax = xmax;
        haveXmax = true;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder activeXids(long[] xids) {
        activeXids = xids;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ZomboDBVisibilityQueryParser.NAME);
        builder.field("name", fieldname);

        if (haveXmin)
            builder.field("xmin", xmin);
        if (haveXmax)
            builder.field("xmax", xmax);
        if (activeXids != null && activeXids.length > 0)
            builder.field("active_xids", activeXids);
        if (query != null) {
            builder.field("query");
            query.toXContent(builder, params);
        }
        builder.endObject();
    }
}
