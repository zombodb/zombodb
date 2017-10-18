package com.tcdi.zombodb.postgres;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbDeleteTuplesAction extends BaseRestHandler {
    private ClusterService clusterService;

    @Inject
    public ZombodbDeleteTuplesAction(Settings settings, RestController controller, Client client, ClusterService clusterService) {
        super(settings, controller, client);

        this.clusterService = clusterService;

        controller.registerHandler(POST, "/{index}/_zdb_delete_tuples", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        String index = request.param("index");
        boolean refresh = request.paramAsBoolean("refresh", false);
        List<ActionRequest> trackingRequests = new ArrayList<>();
        BulkRequest bulkRequest = Requests.bulkRequest();
        bulkRequest.refresh(refresh);

        BufferedReader reader = new BufferedReader(new InputStreamReader(request.content().streamInput()));
        String line;
        int cnt = 0;

        while ((line = reader.readLine()) != null) {
            String[] split = line.split(":");
            String ctid = split[0];
            long xid = Long.valueOf(split[1]);
            int cmax = Integer.valueOf(split[2]);
            split = ctid.split("-");
            int blockno = Integer.parseInt(split[0]);
            int offno = Integer.parseInt(split[1]);
            BytesRef quick_lookup = ZombodbBulkAction.encodeXminData(xid, cmax, blockno, offno);

            if (cnt == 0) {
                GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(index).request()).actionGet();
                int shards = Integer.parseInt(indexSettings.getSetting(index, "index.number_of_shards"));
                String[] routingTable = RoutingHelper.getRoutingTable(client, clusterService, index, shards);

                for (String routing : routingTable) {
                    trackingRequests.add(
                            new IndexRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("aborted")
                                    .setRouting(routing)
                                    .setId(String.valueOf(xid))
                                    .setSource("_zdb_xid", xid)
                                    .request()
                    );
                }
            }

            bulkRequest.add(
                    new IndexRequestBuilder(client)
                            .setIndex(index)
                            .setType("xmax")
                            .setVersionType(VersionType.FORCE)
                            .setVersion(xid)
                            .setRouting(ctid)
                            .setId(ctid)
                            .setSource("_xmax", xid, "_cmax", cmax, "_replacement_ctid", ctid, "_zdb_quick_lookup", quick_lookup)
                            .request()
            );

            cnt++;
        }

        bulkRequest.add(trackingRequests);

        BulkResponse response = client.bulk(bulkRequest).actionGet();
        if (response.hasFailures())
            throw new RuntimeException(response.buildFailureMessage());

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.valueOf("ok")));
    }
}
