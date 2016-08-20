package com.tcdi.zombodb.query;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

public class ExpansionQueryParser implements QueryParser {
    public static String NAME = "expansion";

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        String fieldname = null;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[expansion] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("name".equals(currentFieldName)) {
                    fieldname = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[expansion] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (query == null)
            throw new QueryParsingException(parseContext.index(), "[expansion] missing [query]");
        else if (fieldname == null)
            throw new QueryParsingException(parseContext.index(), "[expansion] missing [name]");

        return new ExpansionQuery(fieldname, query);
    }
}
