/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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

import com.tcdi.zombodb.query_parser.QueryRewriter;
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

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class PostgresTIDResponseAction extends BaseRestHandler {

    private static class BinaryTIDResponse {
        byte[] data;
        int many;
        double ttl;

        private BinaryTIDResponse(byte[] data, int many, double ttl) {
            this.data = data;
            this.many = many;
            this.ttl = ttl;
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
        double buildTime = 0, searchTime = 0;

        try {
            parseStart = System.nanoTime();
            query = buildJsonQueryFromRequestContent(client, request, true, false);
            parseEnd = System.nanoTime();

            SearchRequestBuilder builder = new SearchRequestBuilder(client);
            builder.setIndices(query.getIndexName());
            builder.setSize(32768);
            builder.setScroll(TimeValue.timeValueMinutes(10));
            builder.setSearchType(SearchType.SCAN);
            builder.setPreference(request.param("preference"));
            builder.setTrackScores(true);
            builder.setQueryCache(true);
            builder.setFetchSource(false);
            builder.setNoFields();
            builder.setQuery(query.getQueryBuilder());

            long searchStart = System.currentTimeMillis();
            response = client.execute(DynamicSearchActionHelper.getSearchAction(), builder.request()).get();
            searchTime = (System.currentTimeMillis() - searchStart) / 1000D;

            if (response.getTotalShards() != response.getSuccessfulShards())
                throw new Exception(response.getTotalShards() - response.getSuccessfulShards() + " shards failed");

            tids = buildBinaryResponse(client, response);
            many = tids.many;
            buildTime = tids.ttl;

            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", tids.data));
        } catch (Throwable e) {
            logger.error("Problem building response", e);
            throw e;
        } finally {
            long totalEnd = System.nanoTime();
            logger.info("Found " + many + " rows (ttl=" + ((totalEnd - totalStart) / 1000D / 1000D / 1000D) + "s, search=" + searchTime + "s, parse=" + ((parseEnd - parseStart) / 1000D / 1000D / 1000D) + "s, build=" + buildTime + "s)");
        }
    }

    public static QueryAndIndexPair buildJsonQueryFromRequestContent(Client client, RestRequest request, boolean doFullFieldDataLookups, boolean canDoSingleIndex) {
        String queryString = request.content().toUtf8();
        String indexName = request.param("index");

        try {
            QueryBuilder query;

            if (queryString != null && queryString.trim().length() > 0) {
                QueryRewriter qr = QueryRewriter.Factory.create(client, indexName, request.param("preference"), queryString, doFullFieldDataLookups, canDoSingleIndex);
                query = qr.rewriteQuery();
                indexName = qr.getSearchIndexName();
            } else {
                query = matchAllQuery();
            }

            return new QueryAndIndexPair(query, indexName);
        } catch (Exception e) {
            throw new RuntimeException(queryString, e);
        }
    }

    /**
     * All values are encoded in little-endian so that they can be directly
     * copied into memory on x86
     */
    private BinaryTIDResponse buildBinaryResponse(Client client, SearchResponse searchResponse) throws Exception {
        int many = (int) searchResponse.getHits().getTotalHits();

        long start = System.currentTimeMillis();
        byte[] results = new byte[1 + 8 + 4 + (many * 10)];    // NULL + totalhits + maxscore + (many * (sizeof(int4)+sizeof(int2)+sizeof(float4)))
        int offset = 0, maxscore_offset;
        float maxscore = 0;

        results[0] = 0;
        offset++;
        offset += encodeLong(many, results, offset);

        /* once we know the max score, it goes here */
        maxscore_offset = offset;
        offset += encodeFloat(0, results, offset);

        // kick off the first scroll request
        ActionFuture<SearchResponse> future = client.searchScroll(new SearchScrollRequestBuilder(client)
                .setScrollId(searchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(10))
                .request()
        );
        int cnt = 0;
        while (cnt < many) {
            searchResponse = future.get();

            if (searchResponse.getTotalShards() != searchResponse.getSuccessfulShards())
                throw new Exception(searchResponse.getTotalShards() - searchResponse.getSuccessfulShards() + " shards failed");

            if (searchResponse.getHits().getHits().length == 0) {
                throw new Exception("Underflow in buildBinaryResponse:  Expected " + many + ", got " + cnt);
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

                offset += encodeInteger(blockno, results, offset);
                offset += encodeCharacter(rowno, results, offset);
                offset += encodeFloat(score, results, offset);
                cnt++;
            }
        }

        encodeFloat(maxscore, results, maxscore_offset);

        long end = System.currentTimeMillis();
        return new BinaryTIDResponse(results, many, (end-start)/1000D);
    }

    private static int encodeLong(long value, byte[] buffer, int offset) {
        buffer[offset + 7] = (byte) (value >>> 56);
        buffer[offset + 6] = (byte) (value >>> 48);
        buffer[offset + 5] = (byte) (value >>> 40);
        buffer[offset + 4] = (byte) (value >>> 32);
        buffer[offset + 3] = (byte) (value >>> 24);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 0] = (byte) (value >>> 0);

        return 8;
    }

    private static int encodeFloat(float value, byte[] buffer, int offset) {
        return encodeInteger(Float.floatToRawIntBits(value), buffer, offset);
    }

    private static int encodeInteger(int value, byte[] buffer, int offset) {
        buffer[offset + 3] = (byte) ((value >>> 24) & 0xFF);
        buffer[offset + 2] = (byte) ((value >>> 16) & 0xFF);
        buffer[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        buffer[offset + 0] = (byte) ((value >>> 0) & 0xFF);
        return 4;
    }

    private static int encodeCharacter(char value, byte[] buffer, int offset) {
        buffer[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        buffer[offset + 0] = (byte) ((value >>> 0) & 0xFF);
        return 2;
    }
}
