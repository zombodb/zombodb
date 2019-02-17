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
package llc.zombodb.visibility_query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;

class ZomboDBVisibilityQueryParser implements QueryParser<ZomboDBVisibilityQueryBuilder>, Writeable.Reader<ZomboDBVisibilityQueryBuilder> {

    @Override
    public ZomboDBVisibilityQueryBuilder read(StreamInput in) throws IOException {
        return new ZomboDBVisibilityQueryBuilder(in);
    }

    @Override
    public Optional<ZomboDBVisibilityQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        long myXid = -1;
        long xmin = -1;
        long xmax = -1;
        int commandid = -1;
        Set<Long> activeXids = new HashSet<>();

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("active_xids".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        activeXids.add(parser.longValue());
                    }
                }
            } else if (token.isValue()) {
                if ("myxid".equals(currentFieldName)) {
                    myXid = parser.longValue();
                } else if ("xmin".equals(currentFieldName)) {
                    xmin = parser.longValue();
                } else if ("xmax".equals(currentFieldName)) {
                    xmax = parser.longValue();
                } else if ("commandid".equals(currentFieldName)) {
                    commandid = parser.intValue();
                } else {
                    throw new RuntimeException("[zdb visibility] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (xmin == -1)
            throw new RuntimeException("[zdb visibility] missing [xmin]");
        else if (xmax == -1)
            throw new RuntimeException("[zdb visibility] missing [xmax]");

        long[] activeXidsArray = new long[activeXids.size()];
        int i = 0;
        for (Long l : activeXids)
            activeXidsArray[i++] = l;

        return Optional.of(new ZomboDBVisibilityQueryBuilder(myXid, xmin, xmax, commandid, activeXidsArray));
    }
}
