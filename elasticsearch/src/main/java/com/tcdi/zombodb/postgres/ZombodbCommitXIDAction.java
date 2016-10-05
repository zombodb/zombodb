package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.operation.OperationRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.util.internal.ConcurrentHashMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbCommitXIDAction extends BaseRestHandler {

    private ClusterService clusterService;
    private Map<String, String[]> routingTablesByIndex = new ConcurrentHashMap<>();

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
        String[] routingTable = bruteForceRoutingValuesForShards(index, shards);

        BulkRequest bulkRequest = Requests.bulkRequest();
        bulkRequest.refresh(refresh);

        BufferedReader reader = new BufferedReader(new InputStreamReader(rest.content().streamInput()));
        String line;
        while ((line = reader.readLine()) != null) {
            Long xid = Long.valueOf(line);

            for (int i=0; i<shards; i++) {
                bulkRequest.add(
                        new IndexRequestBuilder(client)
                                .setIndex(index)
                                .setType("committed")
                                .setRouting(routingTable[i])
                                .setId(String.valueOf(xid))
                                .setSource("_zdb_committed_xid", xid)
                                .request()
                );
            }

        }

        BulkResponse response = client.bulk(bulkRequest).actionGet();
        if (response.hasFailures())
            throw new RuntimeException(response.buildFailureMessage());

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.valueOf("ok")));
    }

    private String[] bruteForceRoutingValuesForShards(String index, int shards) {
        String key = index+"."+shards;
        String[] routingTable = routingTablesByIndex.get(key);

        if (routingTable != null)
            return routingTable;

        ClusterState clusterState = clusterService.state();
        OperationRouting operationRouting = clusterService.operationRouting();

        routingTable = new String[shards];
        for (int i=0; i<shards; i++) {
            String routing = String.valueOf(i);

            int cnt=0;
            while ( (operationRouting.indexShards(clusterState, index, "committed", routing, null).shardId()).id() != i)
                routing = String.valueOf(i + ++cnt);
            routingTable[i] = routing;
        }

        routingTablesByIndex.put(key, routingTable);
        return routingTable;
    }
}