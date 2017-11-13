package llc.zombodb.fast_terms;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public class FastTermsRequest extends BroadcastRequest<FastTermsRequest> {
    private String[] types;
    private String fieldname;
    private QueryBuilder query;

    static FastTermsRequest from(StreamInput in) throws IOException {
        FastTermsRequest request = new FastTermsRequest();
        request.readFrom(in);
        return request;
    }

    public String[] types() {
        return types;
    }

    public void types(String[] types) {
        this.types = types;
    }

    public void query(QueryBuilder query) {
        this.query = query;
    }

    public QueryBuilder query() {
        return query;
    }

    public String fieldname() {
        return fieldname;
    }

    public void fieldname(String fieldname) {
        this.fieldname = fieldname;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        types = in.readStringArray();
        fieldname = in.readString();
        query = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(types);
        out.writeString(fieldname);
        out.writeNamedWriteable(query);
    }
}
