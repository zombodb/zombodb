package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
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
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
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
        boolean refresh = request.paramAsBoolean("refresh", false);
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
        bulkRequest.refresh(refresh);
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
            if (!response.hasFailures()) {
                response = processTrackingRequests(request, client, trackingRequests, true);
            }
        } else {
            response = processTrackingRequests(request, client, trackingRequests, true);
            if (!response.hasFailures()) {
                response = client.bulk(bulkRequest).actionGet();
            }
        }

        channel.sendResponse(buildResponse(response, JsonXContent.contentBuilder()));
    }

    private List<ActionRequest> cleanupXmax(Client client, BulkResponse deleteResponses) {
        List<ActionRequest> trackingRequests = new ArrayList<>();

        for (BulkItemResponse response : deleteResponses.getItems()) {
            DeleteResponse dr = response.getResponse();
            if (!dr.isFound()) {
                SearchResponse search = client.search(
                        new SearchRequestBuilder(client)
                                .setIndices(dr.getIndex())
                                .setTypes(dr.getType())
                                .setQuery(termQuery("_replacement_ctid", dr.getId()))
                                .request()
                ).actionGet();

                for (SearchHit hit : search.getHits()) {
                    trackingRequests.add(
                            new DeleteRequestBuilder(client)
                                    .setIndex(hit.getIndex())
                                    .setType("xmax")
                                    .setRouting(hit.id())
                                    .setId(hit.id())
                                    .request()
                    );
                }
            }
        }

        return trackingRequests;
    }

    private BulkResponse processTrackingRequests(RestRequest request, Client client, List<ActionRequest> trackingRequests, boolean refresh) {
        if (trackingRequests.isEmpty())
            return new BulkResponse(new BulkItemResponse[0], 0);

        BulkRequest bulkRequest;
        bulkRequest = Requests.bulkRequest();
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.refresh(refresh);
        bulkRequest.requests().addAll(trackingRequests);

        return client.bulk(bulkRequest).actionGet();
    }

    static RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
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

        for (ActionRequest ar : requests) {
            DeleteRequest doc = (DeleteRequest) ar;

            trackingRequests.add(
                    new DeleteRequestBuilder(client)
                            .setIndex(defaultIndex)
                            .setType("xmax")
                            .setRouting(doc.id())
                            .setId(doc.id())
                            .request()
            );

            doc.routing(doc.id());
        }

        return trackingRequests;
    }

    private List<ActionRequest> handleIndexRequests(Client client, List<ActionRequest> requests, String defaultIndex, String defaultType) {
        List<ActionRequest> trackingRequests = new ArrayList<>();
        int cnt=0;
        for (ActionRequest ar : requests) {
            IndexRequest doc = (IndexRequest) ar;
            Map<String, Object> data = doc.sourceAsMap();
            String prev_ctid = (String) data.get("_prev_ctid");
            Number xid = (Number) data.get("_xmin");
            Number sequence = (Number) data.get("_zdb_seq");

            if (prev_ctid != null) {
                // we are inserting a new doc that replaces a previous doc (an UPDATE)
                Number cmin = (Number) data.get("_cmin");

                trackingRequests.add(
                        new IndexRequestBuilder(client)
                                .setIndex(defaultIndex)
                                .setType("xmax")
                                .setVersionType(VersionType.FORCE)
                                .setVersion(xid.longValue())
                                .setRouting(prev_ctid)
                                .setId(prev_ctid)
                                .setSource("_xmax", xid, "_cmax", cmin, "_replacement_ctid", doc.id())
                                .request()
                );
            }

            if (cnt == 0 && sequence.longValue() > -1) {
                GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(defaultIndex).request()).actionGet();
                int shards = Integer.parseInt(indexSettings.getSetting(defaultIndex, "index.number_of_shards"));
                String[] routingTable = RoutingHelper.getRoutingTable(client, clusterService, defaultIndex, shards);

                for (String routing : routingTable) {
                    trackingRequests.add(
                            new IndexRequestBuilder(client)
                                    .setIndex(defaultIndex)
                                    .setType("aborted")
                                    .setRouting(routing)
                                    .setId(String.valueOf(xid))
                                    .setSource("_zdb_xid", xid)
                                    .request()
                    );
                }
            }

            // every doc with an "_id" that is a ctid needs a version
            // and that version must be *larger* than the document that might
            // have previously occupied this "_id" value -- the Postgres transaction id (xid)
            // works just fine for this as it's always increasing
            doc.opType(IndexRequest.OpType.CREATE);
            doc.version(xid.longValue());
            doc.versionType(VersionType.FORCE);
            doc.routing(doc.id());

            cnt++;
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