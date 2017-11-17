/*
 * Copyright 2017 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package llc.zombodb.rest;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RoutingHelper {
    private static final Map<String, String[]> ROUTING_TABLE = new ConcurrentHashMap<>();

    public static String[] getRoutingTable(ClusterService clusterService, String index, int shards) {
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
            while ( (operationRouting.indexShards(clusterState, index, "aborted", routing).shardId()).id() != i)
                routing = String.valueOf(i + ++cnt);
            routingTable[i] = routing;
        }

        ROUTING_TABLE.put(key, routingTable);
        return routingTable;

    }
}
