/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2017 ZomboDB, LLC
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
package com.tcdi.zombodb.postgres;

import com.tcdi.zombodb.query_parser.ASTLimit;
import com.tcdi.zombodb.query_parser.rewriters.QueryRewriter;
import com.tcdi.zombodb.query_parser.utils.Utils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class PostgresTIDResponseAction extends BaseRestHandler {

    static class TidArrayQuickSort {

        byte[] tmp = new byte[10];
        void quickSort(byte[] array, int offset, int low, int high) {

            if (high <= low)
                return;

            int i = low;
            int j = high;
            int pivot = Utils.decodeInteger(array, offset + ((low+(high-low)/2) * 10));
            while (i <= j) {
                while (Utils.decodeInteger(array, offset+i*10) < pivot)
                    i++;
                while (Utils.decodeInteger(array, offset+j*10) > pivot)
                    j--;
                if (i <= j) {
                    System.arraycopy(array, offset+i*10, tmp, 0, 10);
                    System.arraycopy(array, offset+j*10, array, offset+i*10, 10);
                    System.arraycopy(tmp, 0, array, offset+j*10, 10);
                    i++;
                    j--;
                }
            }
            if (low < j)
                quickSort(array, offset, low, j);
            if (i < high)
                quickSort(array, offset, i, high);
        }
    }

    private static class BinaryTIDResponse {
        byte[] data;
        int many;
        double ttl;
        double sort;

        private BinaryTIDResponse(byte[] data, int many, double ttl, double sort) {
            this.data = data;
            this.many = many;
            this.ttl = ttl;
            this.sort = sort;
        }
    }

    public static final int INVALID_BLOCK_NUMBER = 0xFFFFFFFF;


    @Inject
    public PostgresTIDResponseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_pgtid", this);
        controller.registerHandler(POST, "/{index}/_pgtid", this);
    }


    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        long totalStart = System.nanoTime();
        SearchResponse response;
        BinaryTIDResponse tids;
        QueryAndIndexPair query;
        int many = -1;
        long parseStart = 0, parseEnd = 0;
        double buildTime = 0, searchTime = 0, sortTime = 0;

        try {
            parseStart = System.nanoTime();
            query = buildJsonQueryFromRequestContent(client, request, true, false, false);
            parseEnd = System.nanoTime();

            SearchRequestBuilder builder = new SearchRequestBuilder(client);
            builder.setIndices(query.getIndexName());
            builder.setTypes("data");
            builder.setPreference(request.param("preference"));
            builder.setTrackScores(true);
            builder.setQueryCache(true);
            builder.setFetchSource(false);
            builder.setNoFields();
            builder.setQuery(query.getQueryBuilder());

            if (query.hasLimit()) {
                builder.setSearchType(SearchType.DEFAULT);
                builder.addSort(query.getLimit().getFieldname(), "asc".equals(query.getLimit().getSortDirection()) ? SortOrder.ASC : SortOrder.DESC);
                builder.setFrom(query.getLimit().getOffset());
                builder.setSize(query.getLimit().getLimit());
            } else {
                builder.setSearchType(SearchType.SCAN);
                builder.setScroll(TimeValue.timeValueMinutes(10));
                builder.setSize(32768);
            }

            long searchStart = System.currentTimeMillis();
            response = client.execute(DynamicSearchActionHelper.getSearchAction(), builder.request()).get();
            searchTime = (System.currentTimeMillis() - searchStart) / 1000D;

            if (response.getTotalShards() != response.getSuccessfulShards())
                throw new Exception(response.getTotalShards() - response.getSuccessfulShards() + " shards failed");

            tids = buildBinaryResponse(client, response, query.hasLimit());
            many = tids.many;
            buildTime = tids.ttl;
            sortTime = tids.sort;

            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", tids.data));
        } catch (Throwable e) {
            logger.error("Problem building response", e);
            throw e;
        } finally {
            long totalEnd = System.nanoTime();
            logger.info("Found " + many + " rows (ttl=" + ((totalEnd - totalStart) / 1000D / 1000D / 1000D) + "s, search=" + searchTime + "s, parse=" + ((parseEnd - parseStart) / 1000D / 1000D / 1000D) + "s, build=" + buildTime + "s, sort=" + sortTime + ")");
        }
    }

    public static QueryAndIndexPair buildJsonQueryFromRequestContent(Client client, RestRequest request, boolean doFullFieldDataLookups, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel) {
        String queryString = request.content().toUtf8();
        String indexName = request.param("index");
System.err.println(queryString);
        try {
            QueryBuilder query;
            ASTLimit limit;

            if (queryString != null && queryString.trim().length() > 0) {
                QueryRewriter qr = QueryRewriter.Factory.create(client, indexName, request.param("preference"), queryString, doFullFieldDataLookups, canDoSingleIndex, needVisibilityOnTopLevel);
                query = qr.rewriteQuery();
                indexName = qr.getSearchIndexName();
                limit = qr.getLimit();
            } else {
                query = matchAllQuery();
                limit = null;
            }

            return new QueryAndIndexPair(query, indexName, limit);
        } catch (Exception e) {
            throw new RuntimeException(queryString, e);
        }
    }

    /**
     * All values are encoded in little-endian so that they can be directly
     * copied into memory on x86
     */
    private BinaryTIDResponse buildBinaryResponse(Client client, SearchResponse searchResponse, boolean hasLimit) throws Exception {
        int many = hasLimit ? searchResponse.getHits().getHits().length : (int) searchResponse.getHits().getTotalHits();

        long start = System.currentTimeMillis();
        byte[] results = new byte[1 + 8 + 4 + (many * 10)];    // NULL + totalhits + maxscore + (many * (sizeof(int4)+sizeof(int2)+sizeof(float4)))
        int offset = 0, maxscore_offset, first_byte;
        float maxscore = 0;

        results[0] = 0;
        offset++;
        offset += Utils.encodeLong(many, results, offset);

        /* once we know the max score, it goes here */
        maxscore_offset = offset;
        offset += Utils.encodeFloat(0, results, offset);
        first_byte = offset;

        // kick off the first scroll request
        ActionFuture<SearchResponse> future;

        if (hasLimit)
            future = null;
        else
            future = client.searchScroll(new SearchScrollRequestBuilder(client)
                    .setScrollId(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(10))
                    .request()
            );

        int cnt = 0;
        while (cnt < many) {
            if (future != null)
                searchResponse = future.get();

            if (searchResponse.getTotalShards() != searchResponse.getSuccessfulShards())
                throw new Exception(searchResponse.getTotalShards() - searchResponse.getSuccessfulShards() + " shards failed");

            if (future != null) {
                if (searchResponse.getHits().getHits().length == 0) {
                    throw new Exception("Underflow in buildBinaryResponse:  Expected " + many + ", got " + cnt);
                }
            }

            if (cnt + searchResponse.getHits().getHits().length < many) {
                // go ahead and do the next scroll request
                // while we walk the hits of this chunk
                future = client.searchScroll(new SearchScrollRequestBuilder(client)
                        .setScrollId(searchResponse.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(10))
                        .listenerThreaded(true)
                        .request()
                );
            }

            for (SearchHit hit : searchResponse.getHits()) {
                String id;
                float score;
                int blockno;
                char rowno;

                try {
                    id = hit.id();
                    score = hit.score();

                    int dash = id.indexOf('-', 1);
                    blockno = Integer.parseInt(id.substring(0, dash), 10);
                    rowno = (char) Integer.parseInt(id.substring(dash + 1), 10);
                } catch (Exception nfe) {
                    logger.warn("hit.id()=/" + hit.id() + "/ is not in the proper format.  Defaulting to INVALID_BLOCK_NUMBER");
                    blockno = INVALID_BLOCK_NUMBER;
                    rowno = 0;
                    score = 0;
                }

                if (score > maxscore)
                    maxscore = score;

                offset += Utils.encodeInteger(blockno, results, offset);
                offset += Utils.encodeCharacter(rowno, results, offset);
                offset += Utils.encodeFloat(score, results, offset);
                cnt++;
            }
        }

        Utils.encodeFloat(maxscore, results, maxscore_offset);
        long end = System.currentTimeMillis();

        long sortStart = System.currentTimeMillis();
        new TidArrayQuickSort().quickSort(results, first_byte, 0, many-1);
        long sortEnd = System.currentTimeMillis();

        return new BinaryTIDResponse(results, many, (end - start) / 1000D, (sortEnd - sortStart)/1000D);
    }
}
