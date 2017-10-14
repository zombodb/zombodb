package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.operation.OperationRouting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RoutingHelper {
    private static Map<String, String[]> ROUTING_TABLE = new ConcurrentHashMap<>();

    public static String[] getRoutingTable(Client client, ClusterService clusterService, String index, int shards) {
        String key = index+"."+shards;

        String[] routingTable = ROUTING_TABLE.get(key);

        if (routingTable != null)
            return routingTable;

        ClusterState clusterState = clusterService.state();
        OperationRouting operationRouting = clusterService.operationRouting();

        routingTable = new String[shards];
        for (int i=0; i<shards; i++) {
            String routing = String.valueOf(i);

            int cnt=0;
            while ( (operationRouting.indexShards(clusterState, index, "aborted", routing, null).shardId()).id() != i)
                routing = String.valueOf(i + ++cnt);
            routingTable[i] = routing;
        }

        ROUTING_TABLE.put(key, routingTable);
        return routingTable;

    }
}
