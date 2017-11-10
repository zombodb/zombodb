package llc.zombodb.fast_terms;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class FastTermsAction extends Action<FastTermsRequest, FastTermsResponse, FastTermsRequestBuilder> {

    public static final FastTermsAction INSTANCE = new FastTermsAction();

    public static final String NAME = "indices/fastterms";

    private FastTermsAction() {
        super(NAME);
    }

    @Override
    public FastTermsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new FastTermsRequestBuilder(client, this);
    }

    @Override
    public FastTermsResponse newResponse() {
        return new FastTermsResponse();
    }
}
