/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2019 ZomboDB, LLC
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
package llc.zombodb.rest.search;

import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.FastTermsResponse;
import llc.zombodb.query_parser.ASTLimit;
import llc.zombodb.query_parser.rewriters.QueryRewriter;
import llc.zombodb.query_parser.utils.Utils;
import llc.zombodb.rest.QueryAndIndexPair;
import llc.zombodb.utils.LongIterator;
import llc.zombodb.utils.NumberArrayLookupMergeSortIterator;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBTIDResponseAction extends BaseRestHandler {

    static class TidArrayQuickSort {

        final byte[] tmp = new byte[10];

        void quickSort(byte[] array, int offset, int low, int high, int size) {

            if (high <= low)
                return;

            int i = low;
            int j = high;
            int pivot = Utils.decodeInteger(array, offset + ((low + (high - low) / 2) * size));
            while (i <= j) {
                while (Utils.decodeInteger(array, offset + i * size) < pivot)
                    i++;
                while (Utils.decodeInteger(array, offset + j * size) > pivot)
                    j--;
                if (i <= j) {
                    System.arraycopy(array, offset + i * size, tmp, 0, size);
                    System.arraycopy(array, offset + j * size, array, offset + i * size, size);
                    System.arraycopy(tmp, 0, array, offset + j * size, size);
                    i++;
                    j--;
                }
            }
            if (low < j)
                quickSort(array, offset, low, j, size);
            if (i < high)
                quickSort(array, offset, i, high, size);
        }
    }

    private static class BinaryTIDResponse {
        final byte[] data;
        final int many;
        final double ttl;
        final double sort;

        private BinaryTIDResponse(byte[] data, int many, double ttl, double sort) {
            this.data = data;
            this.many = many;
            this.ttl = ttl;
            this.sort = sort;
        }
    }

    @Inject
    public ZomboDBTIDResponseAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/{index}/_pgtid", this);
        controller.registerHandler(POST, "/{index}/_pgtid", this);
    }


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean wantScores = request.paramAsBoolean("scores", true);
        boolean needSort = request.paramAsBoolean("sort", true);
        long totalStart = System.nanoTime();
        BinaryTIDResponse tids;
        QueryAndIndexPair query;
        int many = -1;
        long parseStart = 0, parseEnd = 0;
        double buildTime = -1, searchTime = -1, sortTime = 0;

        try {
            parseStart = System.nanoTime();
            query = buildJsonQueryFromRequestContent(client, request, false, false, wantScores);
            parseEnd = System.nanoTime();

            if (!wantScores && !query.hasLimit()) {
                // we don't want scores and we don't have a limit to apply
                // so we can get the matching _zdb_id values very quickly
                // using FastTermsAction
                long searchStart = System.currentTimeMillis();
                FastTermsResponse response = FastTermsAction.INSTANCE.newRequestBuilder(client)
                        .setIndices(query.getIndexName())
                        .setTypes("data")
                        .setFieldname("_zdb_id")
                        .setQuery(query.getQueryBuilder())
                        .get();
                searchTime = (System.currentTimeMillis() - searchStart) / 1000D;

                if (response.getFailedShards() > 0) {
                    /* didn't work, so return failure */
                    XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).prettyPrint();
                    response.toXContent(builder, new ToXContent.MapParams(Collections.emptyMap()));
                    return channel -> channel.sendResponse(new BytesRestResponse(response.status(), builder));
                }

                tids = buildBinaryResponse(response);
            } else {
                // we need to run an actual search because we want scores or have a limit
                SearchRequestBuilder builder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
                builder.setIndices(query.getIndexName());
                builder.setTypes("data");
                builder.setPreference(request.param("preference"));
                builder.setTrackScores(query.wantScores()); // query.wantScores() accounts for a #limit(_score)
                builder.setRequestCache(true);
                builder.addDocValueField("_zdb_id");    // this is the only field we need
                builder.addStoredField("_none_");       // don't get any _underscore fields like _id

                if (wantScores)
                    builder.setQuery(query.getQueryBuilder());
                else
                    builder.setPostFilter(query.getQueryBuilder());

                if (query.hasLimit()) {
                    builder.setSearchType(SearchType.DEFAULT);
                    for (ASTLimit.Sorts sort: query.getLimit().getSorts())
                        builder.addSort(sort.fieldname, "asc".equals(sort.direction) ? SortOrder.ASC : SortOrder.DESC);
                    builder.setFrom(query.getLimit().getOffset());
                    builder.setSize(query.getLimit().getLimit());
                } else {
                    if (needSort) {
                        builder.addSort("_zdb_id", SortOrder.ASC);
                        // we don't need to sort the results because we just asked ES to do it for us
                        needSort = false;
                    }

                    builder.setScroll(TimeValue.timeValueMinutes(10));
                    builder.setSize(32768);
                }

                long searchStart = System.currentTimeMillis();
                SearchResponse response = client.search(builder.request()).actionGet();
                searchTime = (System.currentTimeMillis() - searchStart) / 1000D;

                tids = buildBinaryResponse(client, response, query.hasLimit(), wantScores, needSort);
            }

            many = tids.many;
            buildTime = tids.ttl;
            sortTime = tids.sort;

            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", tids.data));

        } finally {
            long totalEnd = System.nanoTime();
            logger.info("Found " + many + " rows (ttl=" + ((totalEnd - totalStart) / 1000D / 1000D / 1000D) + "s, search=" + searchTime + "s, parse=" + ((parseEnd - parseStart) / 1000D / 1000D / 1000D) + "s, build=" + buildTime + "s, sort=" + sortTime + ")");
        }
    }

    public static QueryAndIndexPair buildJsonQueryFromRequestContent(Client client, RestRequest request, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel, boolean wantScores) {
        String queryString = request.content().utf8ToString();
        String indexName = request.param("index");

        try {
            if (queryString != null && queryString.trim().length() > 0) {
                // silence an error from Elasticsearch about an unused request parameter
                // TODO:  should we use 'preference' in FastTerms and/or CrossJoin somehow?
                String preference = request.param("preference");
                QueryRewriter qr = QueryRewriter.Factory.create(request, client, indexName, queryString, canDoSingleIndex, needVisibilityOnTopLevel, wantScores);
                return new QueryAndIndexPair(qr.rewriteQuery(), qr.getVisibilityFilter(), qr.getSearchIndexName(), qr.getLimit(), qr.wantScores());
            } else {
                return new QueryAndIndexPair(matchAllQuery(), matchAllQuery(), indexName, null, wantScores);
            }
        } catch (Exception e) {
            throw new RuntimeException(queryString, e);
        }
    }

    /**
     * All values are encoded in little-endian so that they can be directly
     * copied into memory on x86
     */
    private BinaryTIDResponse buildBinaryResponse(Client client, SearchResponse searchResponse, boolean hasLimit, boolean wantScores, boolean needSort) {
        int many = hasLimit ? searchResponse.getHits().getHits().length : (int) searchResponse.getHits().getTotalHits();

        long start = System.currentTimeMillis();
        byte[] bytes = new byte[
                1 +                             // always NULL
                        8 +                             // number of hits
                        (wantScores ? 4 : 0) +          // max score, if we want scores
                        (many * (wantScores ? 10 : 6))  // size per row, with or without scores sizeof(BlockNumber) + sizeof(OffsetNumber) + ?sizeOf(float4)
                ];
        int offset = 1, first_byte;

        offset += Utils.encodeLong(many, bytes, offset);
        if (wantScores) {
            offset += Utils.encodeFloat(searchResponse.getHits().getMaxScore(), bytes, offset);
        }
        first_byte = offset;

        // kick off the first scroll request
        ActionFuture<SearchResponse> future = null;

        if (searchResponse.getHits().getHits().length == 0)
            future = client.searchScroll(new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                    .setScrollId(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(10))
                    .request()
            );

        int cnt = 0;
        while (cnt < many) {
            if (future != null)
                searchResponse = future.actionGet();

            if (searchResponse.getTotalShards() != searchResponse.getSuccessfulShards())
                throw new RuntimeException(searchResponse.getTotalShards() - searchResponse.getSuccessfulShards() + " shards failed");

            if (future != null) {
                if (searchResponse.getHits().getHits().length == 0) {
                    throw new RuntimeException("Underflow in buildBinaryResponse:  Expected " + many + ", got " + cnt);
                }
            }

            if (cnt + searchResponse.getHits().getHits().length < many) {
                // go ahead and do the next scroll request
                // while we walk the hits of this chunk
                future = client.searchScroll(new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                        .setScrollId(searchResponse.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(10))
                        .request()
                );
            }

            for (SearchHit hit : searchResponse.getHits()) {
                long _zdb_id = hit.getField("_zdb_id").getValue();
                int blockno = (int) (_zdb_id >> 32);
                char offno = (char) _zdb_id;

                if (offno == 0)
                    throw new RuntimeException("Invalid offset number");

                offset += Utils.encodeInteger(blockno, bytes, offset);
                offset += Utils.encodeCharacter(offno, bytes, offset);
                if (wantScores) {
                    offset += Utils.encodeFloat(hit.getScore(), bytes, offset);
                }
                cnt++;
            }
        }

        long end = System.currentTimeMillis();

        long sortStart, sortEnd;

        if (needSort) {
            sortStart = System.currentTimeMillis();
            new TidArrayQuickSort().quickSort(bytes, first_byte, 0, many - 1, wantScores ? 10 : 6);
            sortEnd = System.currentTimeMillis();
        } else {
            // so that we log a sort time of -1.0, indicating we didn't do it
            sortStart = 1000;
            sortEnd = 0;
        }

        return new BinaryTIDResponse(bytes, many, (end - start) / 1000D, (sortEnd - sortStart) / 1000D);
    }

    private BinaryTIDResponse buildBinaryResponse(FastTermsResponse response) {
        int many = response.getDocCount();

        long start = System.currentTimeMillis();
        byte[] bytes = new byte[
                1 +                             // always NULL
                        8 +                     // number of hits
                        (many * 6)  // size per row, with or without scores sizeof(BlockNumber) + sizeof(OffsetNumber) + ?sizeOf(float4)
                ];
        int offset = 1, first_byte;

        offset += Utils.encodeLong(many, bytes, offset);
        first_byte = offset;

        int idx = 0;
        for (LongIterator itr = new NumberArrayLookupMergeSortIterator(response.getNumberLookup()); itr.hasNext();) {
            long _zdb_id = itr.next();
            int blockno = (int) (_zdb_id >> 32);
            char offno = (char) _zdb_id;

            if (offno == 0)
                throw new RuntimeException("Invalid offset number");

            Utils.encodeInteger(blockno, bytes, first_byte + (idx * 6));
            Utils.encodeCharacter(offno, bytes, first_byte + (idx * 6) + 4);
            idx++;
        }
        long end = System.currentTimeMillis();

        return new BinaryTIDResponse(bytes, many, (end - start) / 1000D, -1.0D);
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
