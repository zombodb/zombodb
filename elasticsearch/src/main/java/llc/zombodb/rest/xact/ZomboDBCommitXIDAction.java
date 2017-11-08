package llc.zombodb.rest.xact;

import llc.zombodb.rest.RoutingHelper;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBCommitXIDAction extends BaseRestHandler {

    private ClusterService clusterService;

    @Inject
    public ZomboDBCommitXIDAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);

        this.clusterService = clusterService;

        controller.registerHandler(POST, "/{index}/_zdbxid", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        boolean refresh = request.paramAsBoolean("refresh", false);
        GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(index).request()).actionGet();
        int shards = Integer.parseInt(indexSettings.getSetting(index, "index.number_of_shards"));
        String[] routingTable = RoutingHelper.getRoutingTable(clusterService, index, shards);

        BulkRequest bulkRequest = Requests.bulkRequest();
        bulkRequest.setRefreshPolicy(refresh ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.NONE);

        BufferedReader reader = new BufferedReader(new InputStreamReader(request.content().streamInput()));
        String line;
        while ((line = reader.readLine()) != null) {
            Long xid = Long.valueOf(line);

            for (String routing : routingTable) {
                bulkRequest.add(
                        DeleteAction.INSTANCE.newRequestBuilder(client)
                                .setIndex(index)
                                .setType("aborted")
                                .setRouting(routing)
                                .setId(String.valueOf(xid))
                                .request()
                );
            }

        }

        BulkResponse response = client.bulk(bulkRequest).actionGet();
        if (response.hasFailures())
            throw new RuntimeException(response.buildFailureMessage());

        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.valueOf("ok")));
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}