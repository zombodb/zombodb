package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbCommitXIDAction extends BaseRestHandler {

    private ClusterService clusterService;

    @Inject
    public ZombodbCommitXIDAction(Settings settings, RestController controller, Client client, ClusterService clusterService) {
        super(settings, controller, client);

        this.clusterService = clusterService;

        controller.registerHandler(POST, "/{index}/_zdbxid", this);
    }

    @Override
    protected void handleRequest(RestRequest rest, RestChannel channel, Client client) throws Exception {
        String index = rest.param("index");
        boolean refresh = rest.paramAsBoolean("refresh", false);
        GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(index).request()).actionGet();
        int shards = Integer.parseInt(indexSettings.getSetting(index, "index.number_of_shards"));
        String[] routingTable = RoutingHelper.getRoutingTable(client, clusterService, index, shards);

        BulkRequest bulkRequest = Requests.bulkRequest();
        bulkRequest.refresh(refresh);

        BufferedReader reader = new BufferedReader(new InputStreamReader(rest.content().streamInput()));
        String line;
        while ((line = reader.readLine()) != null) {
            Long xid = Long.valueOf(line);

            for (String routing : routingTable) {
                bulkRequest.add(
                        new IndexRequestBuilder(client)
                                .setIndex(index)
                                .setType("committed")
                                .setRouting(routing)
                                .setId(String.valueOf(xid))
                                .setSource("_zdb_xid", xid)
                                .request()
                );
            }

        }

        BulkResponse response = client.bulk(bulkRequest).actionGet();
        if (response.hasFailures())
            throw new RuntimeException(response.buildFailureMessage());

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.valueOf("ok")));
    }
}