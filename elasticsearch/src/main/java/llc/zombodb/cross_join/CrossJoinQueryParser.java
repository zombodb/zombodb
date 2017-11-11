package llc.zombodb.cross_join;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;

import java.io.IOException;
import java.util.Optional;

public class CrossJoinQueryParser implements QueryParser<CrossJoinQueryBuilder>, Writeable.Reader<CrossJoinQueryBuilder>  {

    @Override
    public CrossJoinQueryBuilder read(StreamInput in) throws IOException {
        return new CrossJoinQueryBuilder(in);
    }

    @Override
    public Optional<CrossJoinQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String clusterName = null;
        String host = null;
        int port = -1;
        String index = null;
        String type = null;
        String leftFieldname = null;
        String rightFieldname = null;
        QueryBuilder query = null;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName != null) {
                if (token.isValue()) {
                    switch (currentFieldName) {
                        case "cluster_name":
                            clusterName = parser.text();
                            break;
                        case "host":
                            host = parser.text();
                            break;
                        case "port":
                            port = parser.intValue();
                            break;
                        case "index":
                            index = parser.text();
                            break;
                        case "type":
                            type = parser.text();
                            break;
                        case "left_fieldname":
                            leftFieldname = parser.text();
                            break;
                        case "right_fieldname":
                            rightFieldname = parser.text();
                            break;
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    switch (currentFieldName) {
                        case "query":
                            query = parseContext.parseInnerQueryBuilder().orElse(null);
                            break;
                    }
                }
            }
        }

        return Optional.of(new CrossJoinQueryBuilder(clusterName, host, port, index, type, leftFieldname, rightFieldname, query));
    }
}
