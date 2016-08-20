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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

public class ZomboDBVisibilityQueryParser implements QueryParser {
    public static String NAME = "zombodb_visibility";

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        String fieldname = null;
        long xid = -1;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[zdb visibility] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("name".equals(currentFieldName)) {
                    fieldname = parser.text();
                } else if ("xid".equals(currentFieldName)) {
                    xid = parser.longValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[zdb visibility] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (query == null)
            throw new QueryParsingException(parseContext.index(), "[zdb visibility] missing [query]");
        else if (fieldname == null)
            throw new QueryParsingException(parseContext.index(), "[zdb visibility] missing [name]");
        else if (xid == -1)
            throw new QueryParsingException(parseContext.index(), "[zdb visibility] missing [xid]");

        return new ZomboDBVisibilityQuery(fieldname, xid, query);
    }
}
