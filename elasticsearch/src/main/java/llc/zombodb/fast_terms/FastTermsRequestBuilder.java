package llc.zombodb.fast_terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;

public class FastTermsRequestBuilder extends BroadcastOperationRequestBuilder<FastTermsRequest, FastTermsResponse, FastTermsRequestBuilder> {

    public FastTermsRequestBuilder(ElasticsearchClient client, FastTermsAction action) {
        super(client, action, new FastTermsRequest());
    }

    public FastTermsRequestBuilder setQuery(QueryBuilder query) {
        request.query(query);
        return this;
    }

    public FastTermsRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    public FastTermsRequestBuilder setFieldname(String fieldname) {
        request.fieldname(fieldname);
        return this;
    }

    @Override
    public void execute(ActionListener<FastTermsResponse> listener) {
        client.execute(FastTermsAction.INSTANCE, this.request(), listener);
    }
}
