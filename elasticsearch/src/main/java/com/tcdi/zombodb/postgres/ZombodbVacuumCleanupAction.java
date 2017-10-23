package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tcdi.zombodb.postgres.ZombodbBulkAction.buildResponse;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbVacuumCleanupAction extends BaseRestHandler {

    private final ClusterService clusterService;

    @Inject
    public ZombodbVacuumCleanupAction(Settings settings, RestController controller, Client client, ClusterService clusterService) {
        super(settings, controller, client);

        this.clusterService = clusterService;

        controller.registerHandler(POST, "/{index}/_zdbvacuumcleanup", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        String index = request.param("index");
        Set<Long> xids = new HashSet<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(request.content().streamInput()));
        String line;

        while ((line = br.readLine()) != null) {
            xids.add(Long.valueOf(line));
        }

        if (!xids.isEmpty()) {
            List<ActionRequest> xmaxRequests = new ArrayList<>();
            List<ActionRequest> abortedRequests = new ArrayList<>();

            filterXidsByDataXmin(client, index, xids);
            cleanupXmax(client, index, xids, xmaxRequests, abortedRequests);

            for (List<ActionRequest> requests : new List[]{xmaxRequests, abortedRequests}) {
                if (!requests.isEmpty()) {
                    BulkRequest bulkRequest = new BulkRequest();
                    BulkResponse response;

                    bulkRequest.refresh(request.paramAsBoolean("refresh", false));
                    bulkRequest.requests().addAll(requests);

                    response = client.bulk(bulkRequest).actionGet();
                    if (response.hasFailures()) {
                        channel.sendResponse(buildResponse(response, JsonXContent.contentBuilder()));
                        return;
                    }
                }
            }
        }

        channel.sendResponse(buildResponse(new BulkResponse(new BulkItemResponse[0], 0), JsonXContent.contentBuilder()));
    }

    private void cleanupXmax(Client client, String index, Set<Long> xids, List<ActionRequest> xmaxRequests, List<ActionRequest> abortedRequests) {
        GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(index).request()).actionGet();
        int shards = Integer.parseInt(indexSettings.getSetting(index, "index.number_of_shards"));
        String[] routingTable = RoutingHelper.getRoutingTable(client, clusterService, index, shards);

        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("xmax")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .addFieldDataField("_xmax")
                .setQuery(termsQuery("_xmax", xids));

        if (!xids.isEmpty()) {
            int total = 0, cnt = 0;
            SearchResponse response = null;
            while (true) {
                if (response == null) {
                    response = client.execute(SearchAction.INSTANCE, search.request()).actionGet();
                    total = (int) response.getHits().getTotalHits();
                } else {
                    response = client.execute(SearchScrollAction.INSTANCE,
                            new SearchScrollRequestBuilder(client)
                                    .setScrollId(response.getScrollId())
                                    .setScroll(TimeValue.timeValueMinutes(10))
                                    .request()).actionGet();
                }

                for (SearchHit hit : response.getHits()) {
                    Number xmax = hit.field("_xmax").value();

                    // we can delete this "xmax" entry because its _xmax transaction
                    // is known to be aborted by Postgres.  As such, its corresponding
                    // "data" doc is going to be visible to all transactions
                    xmaxRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("xmax")
                                    // it's imperative we set the version here to the same version
                                    // we used when we created this doc (the xid that created it)
                                    // because it's possible that between right here and when
                                    // the cluster performs this individual DeleteRequest, Postgres
                                    // will have decided to re-use this tuple (hit.id) and that
                                    // that tuple was also modified (DELETEd or UPDATEd), which gives
                                    // it a new xmax value with a higher version number.  So then
                                    // having this DeleteRequest actually complete would be deleting something
                                    // that isn't what we currently think it is.
                                    .setVersion(xmax.longValue())
                                    .setRouting(hit.id())
                                    .setId(hit.id())
                                    .request()
                    );

                    cnt++;
                }

                if (cnt == total)
                    break;
            }

            // whatever xids we have remaining are guaranteed aborted
            // and unreferenced so we can remove them
            for (Long xid : xids) {
                for (String routing : routingTable) {
                    abortedRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("aborted")
                                    .setRouting(routing)
                                    .setId(String.valueOf(xid))
                                    .request()
                    );
                }
            }
        }

    }

    private void filterXidsByDataXmin(Client client, String index, Set<Long> xids) {
        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("data")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .addFieldDataField("_xmin")
                .setQuery(termsQuery("_xmin", xids));

        int total = 0, cnt = 0;
        SearchResponse response = null;
        while (true) {
            if (response == null) {
                response = client.execute(SearchAction.INSTANCE, search.request()).actionGet();
                total = (int) response.getHits().getTotalHits();
            } else {
                response = client.execute(SearchScrollAction.INSTANCE,
                        new SearchScrollRequestBuilder(client)
                                .setScrollId(response.getScrollId())
                                .setScroll(TimeValue.timeValueMinutes(10))
                                .request()).actionGet();
            }

            // any xid that is referenced as an _xmin in "data"
            // can't be cleaned up yet.  (auto)VACUUM will eventually
            // delete both the "data" doc and its corresponding "xmax" doc
            for (SearchHit hit : response.getHits()) {
                long xmin = ((Number) hit.field("_xmin").value()).longValue();

                // so remove the xid from our set
                xids.remove(xmin);

                cnt++;
            }

            if (cnt == total)
                break;
        }

    }
}