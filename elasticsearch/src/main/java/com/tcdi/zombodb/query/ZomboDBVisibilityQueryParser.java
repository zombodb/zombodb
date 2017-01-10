/*
 * Copyright 2017 ZomboDB, LLC
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
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        long myXid = -1;
        long xmin = -1;
        long xmax = -1;
        Set<Long> activeXids = new HashSet<>();

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
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("active_xids".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        activeXids.add(parser.longValue());
                    }
                }
            } else if (token.isValue()) {
                if ("name".equals(currentFieldName)) {
                    fieldname = parser.text();
                } else if ("myxid".equals(currentFieldName)) {
                    myXid = parser.longValue();
                } else if ("xmin".equals(currentFieldName)) {
                    xmin = parser.longValue();
                } else if ("xmax".equals(currentFieldName)) {
                    xmax = parser.longValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[zdb visibility] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (query == null)
            throw new QueryParsingException(parseContext.index(), "[zdb visibility] missing [query]");
        else if (fieldname == null)
            throw new QueryParsingException(parseContext.index(), "[zdb visibility] missing [name]");
        else if (xmin == -1)
            throw new QueryParsingException(parseContext.index(), "[zdb visibility] missing [xmin]");

        return new ZomboDBVisibilityQuery(query, fieldname, myXid, xmin, xmax, activeXids);
    }
}
