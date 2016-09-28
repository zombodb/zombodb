package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.IdsFilterBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

import static org.elasticsearch.index.query.FilterBuilders.idsFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class ZombodbBulkAction extends BaseRestHandler {

    private ClusterService clusterService;

    @Inject
    public ZombodbBulkAction(Settings settings, RestController controller, Client client, ClusterService clusterService) {
        super(settings, controller, client);

        this.clusterService = clusterService;
        controller.registerHandler(POST, "/{index}/{type}/_zdbbulk", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        BulkRequest bulkRequest = Requests.bulkRequest();
        bulkRequest.listenerThreaded(false);
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        String defaultRouting = request.param("routing");
        boolean isdelete = false;

        String replicationType = request.param("replication");
        if (replicationType != null) {
            bulkRequest.replicationType(ReplicationType.fromString(replicationType));
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            bulkRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.refresh(request.paramAsBoolean("refresh", bulkRequest.refresh()));
        bulkRequest.add(request.content(), defaultIndex, defaultType, defaultRouting, null, true);

        List<ActionRequest> trackingRequests = new ArrayList<>();
        if (!bulkRequest.requests().isEmpty()) {
            isdelete = bulkRequest.requests().get(0) instanceof DeleteRequest;

            if (isdelete) {
                trackingRequests = handleDeleteRequests(client, bulkRequest.requests(), defaultIndex, defaultType);
            } else {
                trackingRequests = handleIndexRequests(client, bulkRequest.requests(), defaultIndex, defaultType);
            }
        }

        BulkResponse response;

        if (isdelete) {
            response = client.bulk(bulkRequest).actionGet();
            processTrackingRequests(request, client, trackingRequests);
        } else {
            processTrackingRequests(request, client, trackingRequests);
            response = client.bulk(bulkRequest).actionGet();
        }

        channel.sendResponse(buildResponse(response, JsonXContent.contentBuilder()));
    }

    private void processTrackingRequests(RestRequest request, Client client, List<ActionRequest> trackingRequests) {
        if (trackingRequests.isEmpty())
            return;

        BulkRequest bulkRequest;
        bulkRequest = Requests.bulkRequest();
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.refresh(false);
        bulkRequest.requests().addAll(trackingRequests);

        client.bulk(bulkRequest).actionGet(); // we don't really care about the response here
    }

    private RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
        builder.startObject();
        if (response.hasFailures()) {
            builder.field(Fields.TOOK, response.getTookInMillis());
            builder.field(Fields.ERRORS, response.hasFailures());
            builder.startArray(Fields.ITEMS);
            for (BulkItemResponse itemResponse : response) {
                builder.startObject();
                builder.startObject(itemResponse.getOpType());
                builder.field(Fields._INDEX, itemResponse.getIndex());
                builder.field(Fields._TYPE, itemResponse.getType());
                builder.field(Fields._ID, itemResponse.getId());
                long version = itemResponse.getVersion();
                if (version != -1) {
                    builder.field(Fields._VERSION, itemResponse.getVersion());
                }
                if (itemResponse.isFailed()) {
                    builder.field(Fields.STATUS, itemResponse.getFailure().getStatus().getStatus());
                    builder.field(Fields.ERROR, itemResponse.getFailure().getMessage());
                } else {
                    if (itemResponse.getResponse() instanceof DeleteResponse) {
                        DeleteResponse deleteResponse = itemResponse.getResponse();
                        if (deleteResponse.isFound()) {
                            builder.field(Fields.STATUS, RestStatus.OK.getStatus());
                        } else {
                            builder.field(Fields.STATUS, RestStatus.NOT_FOUND.getStatus());
                        }
                        builder.field(Fields.FOUND, deleteResponse.isFound());
                    } else if (itemResponse.getResponse() instanceof IndexResponse) {
                        IndexResponse indexResponse = itemResponse.getResponse();
                        if (indexResponse.isCreated()) {
                            builder.field(Fields.STATUS, RestStatus.CREATED.getStatus());
                        } else {
                            builder.field(Fields.STATUS, RestStatus.OK.getStatus());
                        }
                    } else if (itemResponse.getResponse() instanceof UpdateResponse) {
                        UpdateResponse updateResponse = itemResponse.getResponse();
                        if (updateResponse.isCreated()) {
                            builder.field(Fields.STATUS, RestStatus.CREATED.getStatus());
                        } else {
                            builder.field(Fields.STATUS, RestStatus.OK.getStatus());
                        }
                    }
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();

        return new BytesRestResponse(OK, builder);
    }
    private List<ActionRequest> handleDeleteRequests(Client client, List<ActionRequest> requests, String defaultIndex, String defaultType) {
        List<ActionRequest> trackingRequests = new ArrayList<>();
        IdsFilterBuilder ids = idsFilter(defaultType);
        Map<String, DeleteRequest> lookup = new HashMap<>(requests.size());

        for (ActionRequest ar : requests) {
            DeleteRequest doc = (DeleteRequest) ar;
            ids.addIds(doc.id());

            lookup.put(doc.id(), doc);
        }

        SearchResponse response = client.search(
                new SearchRequestBuilder(client)
                        .setIndices(defaultIndex)
                        .setTypes(defaultType)
                        .setPreference("_primary")
                        .setQuery(filteredQuery(null, ids))
                        .setSize(requests.size())
                        .setTerminateAfter(requests.size())
                        .addField("_prev_ctid")
                        .request()
        ).actionGet();

        for (SearchHit hit : response.getHits()) {
            DeleteRequest doc = lookup.get(hit.id());
            String prevCtid = hit.field("_prev_ctid").getValue();

            if (doc != null) {
                doc.routing(prevCtid);

                trackingRequests.add(
                        new DeleteRequestBuilder(client)
                                .setId(doc.id())
                                .setIndex(defaultIndex)
                                .setType("state")
                                .setRouting(prevCtid)
                                .request()
                );

            }
        }

        return trackingRequests;
    }

    private List<ActionRequest> handleIndexRequests(Client client, List<ActionRequest> requests, String defaultIndex, String defaultType) {
        List<ActionRequest> trackingRequests = new ArrayList<>();
        IdsFilterBuilder ids = idsFilter(defaultType);
        Map<String, IndexRequest> lookup = new HashMap<>(requests.size());

        for (ActionRequest ar : requests) {
            IndexRequest doc = (IndexRequest) ar;
            Map<String, Object> data = doc.sourceAsMap();
            final String prevCtid = (String) data.get("_prev_ctid");

            if (prevCtid == null) {
                // this IndexRequest represents an INSERT
                // and as such its rerouting needs to reference itself+xid
                Number xid = (Number) data.get("_xid");
                String routing = doc.id() + ":" + xid.longValue();

                data.put("_prev_ctid", routing);
                doc.source(data);
                doc.routing(routing);
            } else {
                // this IndexRequest represents an UPDATE
                // so we'll look up its routing value in batch below
                ids.addIds(prevCtid);
                lookup.put(prevCtid, doc);
            }
        }

        if (lookup.isEmpty())
            return Collections.emptyList();

        SearchResponse response = null;
        int retries = 0;
        while(retries <= 1) {

            response = client.search(
                    new SearchRequestBuilder(client)
                            .setIndices(defaultIndex)
                            .setTypes(defaultType)
                            .setPreference("_primary")
                            .setQuery(filteredQuery(null, ids))
                            .setQueryCache(retries == 0)
                            .setSize(lookup.size())
                            .setTerminateAfter(lookup.size())
                            .addField("_prev_ctid")
                            .request()
            ).actionGet();

            if (response.getHits().getHits().length != lookup.size()) {
                // didn't find everything, maybe it's because the index needs to be refreshed
                // so lets do that and try one more time
                client.admin().indices().refresh(Requests.refreshRequest(defaultIndex)).actionGet();
                retries++;
                continue;
            }

            break;
        }

        if (response.getHits().getHits().length != lookup.size())
             throw new RuntimeException("Did not find all previous ctids an UPDATE");

        for (SearchHit hit : response.getHits()) {
            String prevCtid = hit.field("_prev_ctid").getValue();
            IndexRequest doc = lookup.get(hit.id());

            if (doc == null)
                continue;

            Map<String, Object> data = doc.sourceAsMap();
            data.put("_prev_ctid", prevCtid);
            doc.source(data);
            doc.routing(prevCtid);

            trackingRequests.add(
                    new IndexRequestBuilder(client)
                            .setId(hit.id())
                            .setIndex(defaultIndex)
                            .setType("state")
                            .setRouting(prevCtid)
                            .setSource("_ctid", prevCtid)
                            .request()
            );

        }

        return trackingRequests;
    }

    private static final class Fields {
        static final XContentBuilderString ITEMS = new XContentBuilderString("items");
        static final XContentBuilderString ERRORS = new XContentBuilderString("errors");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString FOUND = new XContentBuilderString("found");
    }

}