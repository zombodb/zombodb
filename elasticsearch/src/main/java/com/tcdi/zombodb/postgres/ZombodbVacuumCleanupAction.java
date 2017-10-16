package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.ActionRequest;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tcdi.zombodb.postgres.ZombodbBulkAction.buildResponse;
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
        long xmin = request.paramAsLong("xmin", 0);
        String[] active = request.paramAsStringArray("active", new String[] { "0" });
        BulkRequest bulkRequest = new BulkRequest();
        BulkResponse response;

        bulkRequest.refresh(true);
//        bulkRequest.requests().addAll(cleanupHints(client, index));
//        bulkRequest.requests().addAll(cleanupXmax(client, index, xmin, active));

        if (bulkRequest.requests().size() > 0) {
            response = client.bulk(bulkRequest).actionGet();

            channel.sendResponse(buildResponse(response, JsonXContent.contentBuilder()));
        } else {
            channel.sendResponse(buildResponse(new BulkResponse(new BulkItemResponse[0], 0), JsonXContent.contentBuilder()));
        }
    }

    private List<ActionRequest> cleanupXmax(Client client, String index, long xmin, String[] active) {
        List<ActionRequest> trackingRequests = new ArrayList<>();
        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("xmax")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .addFieldDataField("_replacement_ctid")
                .addFieldDataField("_xmax")
                .setQuery(
                        boolQuery()
                        .must(rangeQuery("_xmax").lt(xmin))
                        .mustNot(termsQuery("_xmax", active))
                );

        int total = 0, cnt = 0;
        SearchResponse response = null;
        Set<Long> xids = null;
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

            //
            // for every entry where _id == _replacement_ctid and the _xmax is not committed, delete it
            // these are DELETEd rows from an ABORTed transaction
            //
            for (SearchHit hit : response.getHits()) {
                String replacementCtid = hit.field("_replacement_ctid").value();
                Number xmax = hit.field("_xmax").value();

                if (xids == null)
                    xids = lookupCommittedXids(client, index);

                if (hit.id().equals(replacementCtid) && !xids.contains(xmax.longValue())) {
                    trackingRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("xmax")
                                    .setRouting(hit.id())
                                    .setId(hit.id())
                                    .request()
                    );
                }

                cnt++;
            }

            if (cnt == total)
                break;
        }

        return trackingRequests;

    }

    private List<ActionRequest> cleanupHints(Client client, String index) {
        List<ActionRequest> trackingRequests = new ArrayList<>();
        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("hints")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .setNoFields();

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

            // for every doc in "hints", see if it exists in xmax[_id] or xmax[_replacement_ctid]
            // if it doesn't exist anywhere, it's known to be fully visible by everyone so we can delete the hint
            for (SearchHit hit : response.getHits()) {
                SearchResponse count = client.search(
                        new SearchRequestBuilder(client)
                                .setIndices(index)
                                .setTypes("xmax")
                                .setSearchType(SearchType.COUNT)
                                .setSize(0)
                                .setQuery(
                                        boolQuery()
                                                .should(idsQuery("xmax").addIds(hit.id()))
                                                .should(termQuery("_replacement_ctid", hit.id()))
                                )
                                .request()
                ).actionGet();

                if (count.getHits().getTotalHits() == 0) {
                    trackingRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(index)
                                    .setType("hints")
                                    .setRouting(hit.id())
                                    .setId(hit.id())
                                    .request()
                    );
                }

                cnt++;
            }

            if (cnt == total)
                break;
        }

        return trackingRequests;
    }

    private Set<Long> lookupCommittedXids(Client client, String index) {
        Set<Long> xids = new HashSet<>();
        SearchRequestBuilder search = new SearchRequestBuilder(client)
                .setIndices(index)
                .setTypes("committed")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .setNoFields();

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

            // for every doc in "hints", see if it exists in xmax[_id] or xmax[_replacement_ctid]
            // if it doesn't exist anywhere, it's known to be fully visible by everyone so we can delete the hint
            for (SearchHit hit : response.getHits()) {
                xids.add(Long.valueOf(hit.getId()));
                cnt++;
            }

            if (cnt == total)
                break;
        }

        return xids;
    }
}

