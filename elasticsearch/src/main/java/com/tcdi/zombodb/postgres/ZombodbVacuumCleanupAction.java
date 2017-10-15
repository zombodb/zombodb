package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class ZombodbVacuumCleanupAction extends BaseRestHandler {

    private final ClusterService clusterService;

    @Inject
    public ZombodbVacuumCleanupAction(Settings settings, RestController controller, Client client, ClusterService clusterService) {
        super(settings, controller, client);

        this.clusterService = clusterService;

        controller.registerHandler(GET, "/{index}/_zdbvacuum_cleanup", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        String index = request.param("index");
        GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(index).request()).actionGet();
        int shards = Integer.parseInt(indexSettings.getSetting(index, "index.number_of_shards"));
        String[] routingTable = RoutingHelper.getRoutingTable(client, clusterService, index, shards);
        long xmin = request.paramAsLong("xmin", 0);
        long xmax = request.paramAsLong("xmax", 0);
        String[] active = request.paramAsStringArray("active", new String[]{"0"});

        client.admin().indices().refresh(new RefreshRequestBuilder(client.admin().indices()).setIndices(index).request()).actionGet();

        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("aborted")
                .setRouting(routingTable[0])
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .setQuery(
                        boolQuery()
                                .must(rangeQuery("_zdb_xid").lt(xmin))
                                .mustNot(rangeQuery("_zdb_xid").gte(xmax))
                                .mustNot(termsQuery("_zdb_xid", active))
                )
                .setNoFields();

        List<DeleteRequest> deleteRequests = new ArrayList<>();
        SearchResponse response = null;
        long cnt = 0, total = 0;
        while (true) {
            if (response == null) {
                response = client.execute(SearchAction.INSTANCE, search.request()).actionGet();
                total = response.getHits().getTotalHits();
            } else {
                response = client.execute(SearchScrollAction.INSTANCE,
                        new SearchScrollRequestBuilder(client)
                                .setScrollId(response.getScrollId())
                                .setScroll(TimeValue.timeValueMinutes(10))
                                .request()).actionGet();
            }

            for (SearchHit hit : response.getHits()) {
                long xid = Long.valueOf(hit.id());

                deleteDataByXid(client, index, xid, deleteRequests);

                //
                // broadcast a delete for this "aborted" xid across all shards
                //
                for (String routing : routingTable) {
                    deleteRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("aborted")
                                    .setRouting(routing)
                                    .setId(String.valueOf(xid))
                                    .request()
                    );
                }


                cnt++;
            }

            if (cnt == total)
                break;
        }

        if (deleteRequests.size() > 0) {
            BulkRequest bulkRequest;
            BulkResponse bulkResponse;

            bulkRequest = Requests.bulkRequest();
            bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
            bulkRequest.refresh(request.paramAsBoolean("refresh", true));
            bulkRequest.requests().addAll(deleteRequests);

            bulkResponse = client.bulk(bulkRequest).actionGet();

            channel.sendResponse(ZombodbBulkAction.buildResponse(bulkResponse, JsonXContent.contentBuilder()));
        } else {
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", "{}"));
        }
    }

    private void deleteDataByXid(Client client, String index, long xid, List<DeleteRequest> deleteRequests) {
        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("data", "xmax")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setQuery(boolQuery().should(termQuery("_xmin", xid)).should(termQuery("_xmax", xid)))
                .setSize(10000);
        SearchResponse response = null;

        long cnt = 0, total = 0;
        while (true) {
            if (response == null) {
                response = client.execute(SearchAction.INSTANCE, search.request()).actionGet();
                total = response.getHits().getTotalHits();
            } else {
                response = client.execute(SearchScrollAction.INSTANCE,
                        new SearchScrollRequestBuilder(client)
                                .setScrollId(response.getScrollId())
                                .setScroll(TimeValue.timeValueMinutes(10))
                                .request()).actionGet();
            }

            for (SearchHit hit : response.getHits()) {
                switch (hit.type()) {
                    case "xmax":
                        deleteRequests.add(
                                new DeleteRequestBuilder(client)
                                        .setIndex(index)
                                        .setType("xmax")
                                        .setRouting(hit.id())
                                        .setId(hit.id())
                                        .request()
                        );
                        break;

                    case "data":
                        deleteRequests.add(
                                new DeleteRequestBuilder(client)
                                        .setIndex(index)
                                        .setType("data")
                                        .setRouting(hit.id())
                                        .setId(hit.id())
                                        .request()
                        );
                        break;

                    default:
                        throw new RuntimeException("Unexpected type: " + hit.type());
                }
                cnt++;
            }

            if (cnt == total)
                break;
        }

    }
}

