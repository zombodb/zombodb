package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
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
import static org.elasticsearch.rest.RestRequest.Method.POST;

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
        String[] active = request.paramAsStringArray("active", new String[] {"0"});

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

                SearchResponse dataRows = client.search(
                        new SearchRequestBuilder(client)
                                .setIndices(index)
                                .setTypes("data")
                                .setSearchType(SearchType.COUNT)
                                .setSize(0)
                                .setQuery(termQuery("_xid", xid))
                                .request()
                ).actionGet();

                if (dataRows.getHits().totalHits() == 0) {
                    // we have no rows with this aborted transaction id
                    // so queue up a delete for the transaction
                    // and then look for and queue up deletes for rows in 'updated' and 'deleted'
                    // that are part of this aborted transaction too

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

                    findDocsOfType(client, routingTable, index, "updated", "_updating_xid", xid, deleteRequests);
                    findDocsOfType(client, routingTable, index, "deleted", "_deleting_xid", xid, deleteRequests);
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

    private void findDocsOfType(Client client, String[] routingTable, String index, String type, String fieldname, long xid, List<DeleteRequest> deleteRequests) {
        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes(type)
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setQuery(termQuery(fieldname, xid))
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
                for (String routing : routingTable) {
                    deleteRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(index)
                                    .setType(type)
                                    .setRouting(routing)
                                    .setId(hit.id())
                            .request()
                    );
                }
                cnt++;
            }

            if (cnt == total)
                break;
        }

    }
}

