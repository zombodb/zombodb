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
package llc.zombodb.rest.xact;

import llc.zombodb.rest.RoutingHelper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBDeleteTuplesAction extends BaseRestHandler {
    private final ClusterService clusterService;

    @Inject
    public ZomboDBDeleteTuplesAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);

        this.clusterService = clusterService;

        controller.registerHandler(POST, "/{index}/_zdb_delete_tuples", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        boolean refresh = request.paramAsBoolean("refresh", false);
        List<DocWriteRequest> xmaxRequests = new ArrayList<>();
        List<DocWriteRequest> abortedRequests = new ArrayList<>();
        String optimizeForJoins = request.param("optimize_for_joins");
        if ("null".equals(optimizeForJoins))
            optimizeForJoins = null;

        BufferedReader reader = new BufferedReader(new InputStreamReader(request.content().streamInput()));
        String line;
        int cnt = 0;

        while ((line = reader.readLine()) != null) {
            String[] split = line.split(":");
            String ctid = split[0];
            long xid = Long.valueOf(split[1]);
            int cmax = Integer.valueOf(split[2]);
            long joinKey = Long.valueOf(split[3]);
            split = ctid.split("-");
            int blockno = Integer.parseInt(split[0]);
            int offno = Integer.parseInt(split[1]);
            BytesRef encodedTuple = ZomboDBBulkAction.encodeTuple(xid, cmax, blockno, offno);

            if (cnt == 0) {
                GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(index).request()).actionGet();
                int shards = Integer.parseInt(indexSettings.getSetting(index, "index.number_of_shards"));
                String[] routingTable = RoutingHelper.getRoutingTable(clusterService, index, shards);

                for (String routing : routingTable) {
                    abortedRequests.add(
                            IndexAction.INSTANCE.newRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("aborted")
                                    .setRouting(routing)
                                    .setId(String.valueOf(xid))
                                    .setSource("_zdb_xid", xid)
                                    .request()
                    );
                }
            }

            xmaxRequests.add(
                    IndexAction.INSTANCE.newRequestBuilder(client)
                            .setIndex(index)
                            .setType("xmax")
                            .setVersionType(VersionType.FORCE)
                            .setVersion(xid)
                            .setRouting(optimizeForJoins != null ? ZomboDBBulkAction.calcRoutingValue(joinKey) : ctid)
                            .setId(ctid)
                            .setSource("_xmax", xid, "_cmax", cmax, "_replacement_ctid", ctid, "_zdb_encoded_tuple", encodedTuple, "_zdb_reason", "D")
                            .request()
            );

            cnt++;
        }

        BulkResponse response;
        for (List<DocWriteRequest> requests : new List[] { abortedRequests, xmaxRequests }){
            BulkRequest bulkRequest = Requests.bulkRequest();
            bulkRequest.setRefreshPolicy(refresh ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.NONE);
            bulkRequest.add(requests);

            response = client.bulk(bulkRequest).actionGet();
            if (response.hasFailures())
                throw new RuntimeException(response.buildFailureMessage());
        }

        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.valueOf("ok")));
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
