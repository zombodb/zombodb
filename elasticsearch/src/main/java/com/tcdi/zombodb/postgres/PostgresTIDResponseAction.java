/*
 * Copyright 2013-2015 Technology Concepts & Design, Inc
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

import com.tcdi.zombodb.postgres.util.OverloadedContentRestRequest;
import com.tcdi.zombodb.postgres.util.QueryAndIndexPair;
import com.tcdi.zombodb.query_parser.QueryRewriter;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Created by e_ridge on 9/4/14.
 */
public class PostgresTIDResponseAction extends BaseRestHandler {

    private static class BinaryTIDResponse {
        byte[] data;
        int many;
        long start;
        long end;

        private BinaryTIDResponse(byte[] data, int many, long start, long end) {
            this.data = data;
            this.many = many;
            this.start = start;
            this.end = end;
        }
    }

    public static final int INVALID_BLOCK_NUMBER = 0xFFFFFFFF;


    @Inject
    public PostgresTIDResponseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/{type}/_pgtid", this);
        controller.registerHandler(POST, "/{index}/{type}/_pgtid", this);
    }


    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        final long totalStart = System.nanoTime();
        final long searchEnd;

        final SearchRequest searchRequest;
        final SearchResponse response;
        final BinaryTIDResponse tids;

        final QueryAndIndexPair query;

        try {
            // build a json query from the request content
            // and then change what our request looks like so it'll appear
            // as if the json version is the actual content
            long parseStart = System.nanoTime();
            query = buildJsonQueryFromRequestContent(client, request, false, "data".equals(request.param("type")));
            long parseEnd = System.nanoTime();

            request = new OverloadedContentRestRequest(request, new BytesArray(query.getQuery()));
            request.params().put("index", query.getIndexName());
            request.params().put("type", "data");
            request.params().put("size", "32768");
            request.params().put("_source", "false");
            request.params().put("track_scores", "true");

            // perform the search
            searchRequest = RestSearchAction.parseSearchRequest(request);
            searchRequest.listenerThreaded(false);
            searchRequest.scroll(TimeValue.timeValueMinutes(10));
            searchRequest.searchType(SearchType.SCAN);
            searchRequest.preference(request.param("preference"));

            final long searchStart = System.currentTimeMillis();
            response = client.search(searchRequest).get();

            if (response.getTotalShards() != response.getSuccessfulShards())
                throw new Exception(response.getTotalShards() - response.getSuccessfulShards() + " shards failed");

            searchEnd = System.currentTimeMillis();

            tids = buildBinaryResponse(client, response);
            long totalEnd = System.nanoTime();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", tids.data));
            logger.info("Found " + tids.many + " rows (ttl=" + ((totalEnd - totalStart) / 1000D / 1000D / 1000D) + "s, parse=" + ((parseEnd - parseStart) / 1000D / 1000D / 1000D) + "s, search=" + ((searchEnd - searchStart) / 1000D) + "s, build=" + ((tids.end - tids.start) / 1000D) + "s)");
        } catch (Throwable e) {
            logger.error("Problem building response", e);
            throw e;
        }
    }

    public static QueryAndIndexPair buildJsonQueryFromRequestContent(Client client, RestRequest request, boolean allowSingleIndex, boolean useParentChild) {
        String query = request.content().toUtf8();

        try {
            QueryRewriter qr = new QueryRewriter(client, request.param("index"), request.param("preference"), query, allowSingleIndex, useParentChild);
            String indexName;

            // the request content is just our straight query string
            // so transform it into json
            if (query != null && query.trim().length() > 0) {
                query = qr.rewriteQuery().toString();
                indexName = qr.getSearchIndexName();
            } else {
                query = "{ \"match_all\": {} }";
                indexName = request.param("index");
            }

            return new QueryAndIndexPair(String.format("{ \"query\": %s }", query), indexName);
        } catch (Exception e) {
            throw new RuntimeException(query, e);
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

        int cnt = 0;
        while (cnt < many) {
            searchResponse = client.searchScroll(
                    new SearchScrollRequestBuilder(client)
                            .setScrollId(searchResponse.getScrollId())
                            .setScroll(TimeValue.timeValueMinutes(10))
                            .request()
            ).get();

            if (searchResponse.getTotalShards() != searchResponse.getSuccessfulShards())
                throw new Exception(searchResponse.getTotalShards() - searchResponse.getSuccessfulShards() + " shards failed");

            if (searchResponse.getHits().getHits().length == 0) {
                throw new Exception("Underflow in buildBinaryResponse:  Expected " + many + ", got " + cnt);
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
        return new BinaryTIDResponse(results, many, start, end);
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
