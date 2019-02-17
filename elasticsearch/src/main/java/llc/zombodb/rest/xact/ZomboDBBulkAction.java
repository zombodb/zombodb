package llc.zombodb.rest.xact;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import llc.zombodb.rest.RoutingHelper;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class ZomboDBBulkAction extends BaseRestHandler {

    private final ClusterService clusterService;

    @Inject
    public ZomboDBBulkAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);

        this.clusterService = clusterService;

        controller.registerHandler(POST, "/{index}/{type}/_zdbbulk", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        BulkRequest bulkRequest = Requests.bulkRequest();
        BulkResponse response;

        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        boolean refresh = request.paramAsBoolean("refresh", false);
        boolean isdelete = false;
        int requestNumber = request.paramAsInt("request_no", -1);
        String blockRoutingField = request.param("block_routing_field", null);
        if ("null".equals(blockRoutingField))
            blockRoutingField = null;

        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(refresh ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.NONE);
        bulkRequest.add(request.content(), defaultIndex, defaultType);

        List<DocWriteRequest> xmaxRequests = new ArrayList<>();
        List<DocWriteRequest> abortedRequests = new ArrayList<>();
        if (!bulkRequest.requests().isEmpty()) {
            isdelete = bulkRequest.requests().get(0) instanceof DeleteRequest;

            if (isdelete) {
                handleDeleteRequests(client, blockRoutingField != null, bulkRequest.requests(), defaultIndex, xmaxRequests);
            } else {
                handleIndexRequests(client, blockRoutingField, bulkRequest.requests(), defaultIndex, requestNumber, xmaxRequests, abortedRequests);
            }
        }

        if (isdelete) {
            // when deleting, we need to delete the "data" docs first
            // otherwise VisibilityQueryHelper might think "data" docs don't have an "xmax" when they really do
            response = client.bulk(bulkRequest).actionGet();

            if (!response.hasFailures()) {
                // then we can delete from "xmax"
                response = processTrackingRequests(request, client, xmaxRequests);
            }
        } else {
            // when inserting, we first need to add the "aborted" docs
            response = processTrackingRequests(request, client, abortedRequests);

            if (!response.hasFailures()) {
                // then we need to add the "xmax" docs
                // otherwise VisibilityQueryHelper might think "data" docs don't have an "xmax" when they really do
                response = processTrackingRequests(request, client, xmaxRequests);

                if (!response.hasFailures()) {
                    // then we can insert into "data"
                    response = client.bulk(bulkRequest).actionGet();
                }
            }
        }

        BulkResponse finalResponse = response;
        return channel -> channel.sendResponse(buildResponse(finalResponse, JsonXContent.contentBuilder()));
    }

    private BulkResponse processTrackingRequests(RestRequest request, Client client, List<DocWriteRequest> trackingRequests) {
        if (trackingRequests.isEmpty())
            return new BulkResponse(new BulkItemResponse[0], 0);
        boolean refresh = request.paramAsBoolean("refresh", false);

        BulkRequest bulkRequest;
        bulkRequest = Requests.bulkRequest();
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(refresh ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.NONE);
        bulkRequest.requests().addAll(trackingRequests);
        return client.bulk(bulkRequest).actionGet();
    }

    public static RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
        int errorCnt = 0;
        builder.startObject();
        if (response.hasFailures()) {
            builder.startArray("items");
            main_loop: for (BulkItemResponse itemResponse : response) {
                if (itemResponse.isFailed()) {

                    // handle failure conditions that we know are
                    // okay/expected as if they never happened
                    BulkItemResponse.Failure failure = itemResponse.getFailure();

                    switch (failure.getStatus()) {
                        case CONFLICT:
                            if (failure.getMessage().contains("VersionConflictEngineException")) {
                                if ("xmax".equals(itemResponse.getType())) {
                                    if (itemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                                        // this is a version conflict error where we tried to delete
                                        // an old xmax doc, which is perfectly acceptable
                                        continue main_loop;
                                    }
                                }
                            }
                            break;

                        default:
                            errorCnt++;
                            break;
                    }

                    builder.startObject();
                    builder.startObject(itemResponse.getOpType().name());
                    builder.field("_index", itemResponse.getIndex());
                    builder.field("_type", itemResponse.getType());
                    builder.field("_id", itemResponse.getId());
                    long version = itemResponse.getVersion();
                    if (version != -1) {
                        builder.field("_version", itemResponse.getVersion());
                    }
                    if (itemResponse.isFailed()) {
                        builder.field("status", itemResponse.getFailure().getStatus().getStatus());
                        builder.field("error", itemResponse.getFailure().getMessage());
                    } else {
                        if (itemResponse.getResponse() instanceof DeleteResponse) {
                            DeleteResponse deleteResponse = itemResponse.getResponse();
                            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                                builder.field("status", RestStatus.OK.getStatus());
                                builder.field("found", true);
                            } else {
                                builder.field("status", RestStatus.NOT_FOUND.getStatus());
                                builder.field("found", false);
                            }
                        } else if (itemResponse.getResponse() instanceof IndexResponse) {
                            IndexResponse indexResponse = itemResponse.getResponse();
                            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                                builder.field("status", RestStatus.CREATED.getStatus());
                            } else {
                                builder.field("status", RestStatus.OK.getStatus());
                            }
                        } else if (itemResponse.getResponse() instanceof UpdateResponse) {
                            UpdateResponse updateResponse = itemResponse.getResponse();
                            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                                builder.field("status", RestStatus.CREATED.getStatus());
                            } else {
                                builder.field("status", RestStatus.OK.getStatus());
                            }
                        }
                    }
                    builder.endObject();
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.field("took", response.getTookInMillis());
            if (errorCnt > 0) {
                builder.field("errors", true);
            }
        }
        builder.endObject();

        return new BytesRestResponse(OK, builder);
    }

    private void handleDeleteRequests(Client client, boolean optimizeForJoins, List<DocWriteRequest> requests, String defaultIndex, List<DocWriteRequest> xmaxRequests) {
        if (optimizeForJoins) {
            //
            // we're doing block routing and as such, we have no idea what the "_routing" value
            // is for the documents we want to delete.  We could search up all the documents to find their
            // routing, but it's easier to issue a Delete request to every shard, so that's what we're doing here
            //
            GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(defaultIndex).request()).actionGet();
            int shards = Integer.parseInt(indexSettings.getSetting(defaultIndex, "index.number_of_shards"));
            String[] routingTable = RoutingHelper.getRoutingTable(clusterService, defaultIndex, shards);

            // additional DeleteRequests for each document against shards other than the 0'th shard
            List<DeleteRequest> otherShards = new ArrayList<>();

            for (DocWriteRequest doc : requests) {

                for (int i=0; i<routingTable.length; i++) {
                    // xmax delete requests for each shard
                    xmaxRequests.add(
                            DeleteAction.INSTANCE.newRequestBuilder(client)
                                    .setIndex(defaultIndex)
                                    .setType("xmax")
                                    .setRouting(routingTable[i])
                                    .setId(doc.id())
                                    .request()
                    );

                    if (i == 0) {
                        // first routing value, so just set the routing on the
                        // doc already in the list of requests
                        doc.routing(routingTable[i]);
                    } else {
                        // not-the-first routing value, so create new delete requests
                        // that are the same as the current, but with this routing value
                        otherShards.add(
                                DeleteAction.INSTANCE.newRequestBuilder(client)
                                        .setIndex(doc.index())
                                        .setType(doc.type())
                                        .setId(doc.id())
                                        .setRouting(routingTable[i])
                                        .setVersion(doc.version())
                                        .setVersionType(doc.versionType())
                                        .request()
                        );
                    }
                }
            }

            // remember all the other DeleteRequests we made
            requests.addAll(otherShards);
        } else {
            // we're not doing block routing, and as such we know the routing value
            // for every document -- it's the "id"
            for (DocWriteRequest doc : requests) {
                xmaxRequests.add(
                        DeleteAction.INSTANCE.newRequestBuilder(client)
                                .setIndex(defaultIndex)
                                .setType("xmax")
                                .setRouting(doc.id())
                                .setId(doc.id())
                                .request()
                );

                doc.routing(doc.id());
            }
        }
    }

    private void handleIndexRequests(Client client, String optimizeForJoins, List<DocWriteRequest> requests, String defaultIndex, int requestNumber, List<DocWriteRequest> xmaxRequests, List<DocWriteRequest> abortedRequests) {
        GetSettingsResponse indexSettings = client.admin().indices().getSettings(client.admin().indices().prepareGetSettings(defaultIndex).request()).actionGet();
        int shards = Integer.parseInt(indexSettings.getSetting(defaultIndex, "index.number_of_shards"));
        String[] routingTable = RoutingHelper.getRoutingTable(clusterService, defaultIndex, shards);

        int cnt = 0;
        for (DocWriteRequest ar : requests) {
            IndexRequest doc = (IndexRequest) ar;
            Map<String, Object> data = doc.sourceAsMap();
            String prev_ctid = (String) data.get("_prev_ctid");
            Number prev_organize_for_joins = optimizeForJoins != null ? (Number) data.get("_prev_organize_for_joins") : null;
            Number xmin = (Number) data.get("_xmin");
            Number cmin = (Number) data.get("_cmin");
            Number sequence = (Number) data.get("_zdb_seq");    // -1 means an index build (CREATE INDEX)
            Number routing_key = optimizeForJoins != null ? (Number) data.get(optimizeForJoins) : null;
            String docRouting;

            if (prev_ctid != null) {
                // we are inserting a new doc that replaces a previous doc (an UPDATE)
                String[] parts = prev_ctid.split("-");
                int blockno = Integer.parseInt(parts[0]);
                int offno = Integer.parseInt(parts[1]);
                BytesRef encodedTuple = encodeTuple(xmin.longValue(), cmin.intValue(), blockno, offno);

                if (optimizeForJoins != null && prev_organize_for_joins == null)
                    throw new RuntimeException("Found null routing value in [" + defaultIndex + "] with id [" + doc.id() + "] for field [_prev_organize_for_joins]");

                xmaxRequests.add(
                        IndexAction.INSTANCE.newRequestBuilder(client)
                                .setIndex(defaultIndex)
                                .setType("xmax")
                                .setVersionType(VersionType.FORCE)
                                .setVersion(xmin.longValue())
                                .setRouting(optimizeForJoins != null ? calcRoutingValue(prev_organize_for_joins) : prev_ctid)
                                .setId(prev_ctid)
                                .setSource("_xmax", xmin, "_cmax", cmin, "_replacement_ctid", doc.id(), "_zdb_encoded_tuple", encodedTuple, "_zdb_reason", "U")
                                .request()
                );
            }

            if (sequence.intValue() > -1) {
                // delete a possible existing xmax value for this doc
                // but only if we're NOT in an index build (ie, CREATE INDEX)

                if (optimizeForJoins != null) {
                    // if we are doing block routing then we need to send the delete to every shard
                    // because we don't know what routing value might have been used
                    for (String routing : routingTable) {
                        xmaxRequests.add(
                                DeleteAction.INSTANCE.newRequestBuilder(client)
                                        .setIndex(defaultIndex)
                                        .setType("xmax")
                                        .setRouting(routing)
                                        .setId(doc.id())
                                        .request()
                        );
                    }
                } else {
                    // not doing block routing, so the routing is the doc's "id"
                    xmaxRequests.add(
                            DeleteAction.INSTANCE.newRequestBuilder(client)
                                    .setIndex(defaultIndex)
                                    .setType("xmax")
                                    .setRouting(doc.id())
                                    .setId(doc.id())
                                    .request()
                    );
                }
            }

            // only add the "aborted" xid entry if this is the first
            // record in what might be a batch of inserts from one statement
            if (requestNumber == 0 && cnt == 0 && sequence.intValue() > -1) {
                for (String routing : routingTable) {
                    abortedRequests.add(
                            IndexAction.INSTANCE.newRequestBuilder(client)
                                    .setIndex(defaultIndex)
                                    .setType("aborted")
                                    .setRouting(routing)
                                    .setId(String.valueOf(xmin))
                                    .setSource("_zdb_xid", xmin)
                                    .request()
                    );
                }
            }

            if (optimizeForJoins != null) {
                if (routing_key == null)
                    throw new RuntimeException("Found null routing value in [" + defaultIndex + "] with id [" + doc.id() + "] for field [" + optimizeForJoins + "]");
                docRouting = calcRoutingValue(routing_key);
            } else {
                docRouting = doc.id();
            }

            // every doc with an "_id" that is a ctid needs a version
            // and that version must be *larger* than the document that might
            // have previously occupied this "_id" value -- the Postgres transaction id (xid)
            // works just fine for this as it's always increasing
            doc.opType(IndexRequest.OpType.INDEX);
            doc.version(xmin.longValue());
            doc.versionType(VersionType.FORCE);
            doc.routing(docRouting);

            cnt++;
        }
    }

    static BytesRef encodeTuple(long xid, int cmin, int blockno, int offno) {
        try {
            byte[] tuple = new byte[4 + 2 + 8 + 4];  // blockno + offno + xmax + cmax
            ByteArrayDataOutput out = new ByteArrayDataOutput(tuple);
            out.writeVInt(blockno);
            out.writeVInt(offno);
            out.writeVLong(xid);
            out.writeVInt(cmin);
            return new BytesRef(tuple, 0, out.getPosition());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    static String calcRoutingValue(Number incomingValue) {
        return String.valueOf(incomingValue.longValue()/100_000L);
    }
}