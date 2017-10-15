package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.*;

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
        String pkeyFieldname = lookupPkeyFieldname(client, defaultIndex);

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
            bulkRequest.refresh(false);
            response = client.bulk(bulkRequest).actionGet();
            if (!response.hasFailures())
                response = processTrackingRequests(request, client, trackingRequests);
        } else {
            response = processTrackingRequests(request, client, trackingRequests);
            if (!response.hasFailures())
                response = client.bulk(bulkRequest).actionGet();
        }

        channel.sendResponse(buildResponse(response, JsonXContent.contentBuilder()));
    }

    private BulkResponse processTrackingRequests(RestRequest request, Client client, List<ActionRequest> trackingRequests) {
        if (trackingRequests.isEmpty())
            return new BulkResponse(new BulkItemResponse[0], 0);

        BulkRequest bulkRequest;
        bulkRequest = Requests.bulkRequest();
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.refresh(request.paramAsBoolean("refresh", false));
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
        }

        return trackingRequests;
    }

    private List<ActionRequest> handleIndexRequests(Client client, List<ActionRequest> requests, String defaultIndex, String defaultType) {
        GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(defaultIndex).request()).actionGet();
        int shards = Integer.parseInt(indexSettings.getSetting(defaultIndex, "index.number_of_shards"));

        List<ActionRequest> trackingRequests = new ArrayList<>();
        Set<Number> xids = new HashSet<>();

        for (ActionRequest ar : requests) {
            IndexRequest doc = (IndexRequest) ar;
            Map<String, Object> data = doc.sourceAsMap();
            String prev_ctid = (String) data.get("_prev_ctid");
            Number xid = (Number) data.get("_xmin");

            markXidsAsAborted(client, clusterService, defaultIndex, shards, trackingRequests, xids, xid);

            if (prev_ctid != null) {
                // we are inserting a new doc that replaces a previous doc (an UPDATE)
                // so broadcast that ctid to all shards
                Number cmin = (Number) data.get("_cmin");

                trackingRequests.add(
                        new IndexRequestBuilder(client)
                                .setIndex(defaultIndex)
                                .setType("xmax")
                                .setVersionType(VersionType.FORCE)
                                .setVersion(xid.longValue())
                                .setRouting(prev_ctid)
                                .setId(prev_ctid)
                                .setSource("_xmax", xid, "_cmax", cmin)
                                .request()
                );
            }

            // every doc with an "_id" that is a ctid needs a version
            // and that version must be *larger* than the document that might
            // have previously occupied this "_id" value -- the Postgres transaction id (xid)
            // works just fine for this as it's always increasing
            doc.opType(IndexRequest.OpType.CREATE);
            doc.version(xid.longValue());
            doc.versionType(VersionType.FORCE);
            doc.routing(doc.id());
        }

        return trackingRequests;
    }

    static void markXidsAsAborted(Client client, ClusterService clusterService, String defaultIndex, int shards, List<ActionRequest> trackingRequests, Set<Number> xids, Number xid) {
        if (!xids.contains(xid)) {
            String[] routingTable = RoutingHelper.getRoutingTable(client, clusterService, defaultIndex, shards);
            // add the xid for this record to each shard in the "aborted" type
            // if the transaction commits, then ZombodbCommitXIDAction will be called and
            // they'll be deleted
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

            xids.add(xid);
        }
    }

    private String lookupPkeyFieldname(Client client, String index) {
        GetMappingsResponse mappings = client.admin().indices().getMappings(new GetMappingsRequest().indices(index).types("data")).actionGet();
        MappingMetaData mmd = mappings.getMappings().get(index).get("data");

        try {
            return (String) ((Map) mmd.getSourceAsMap().get("_meta")).get("primary_key");
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
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