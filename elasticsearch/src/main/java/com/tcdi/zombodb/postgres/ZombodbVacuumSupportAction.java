/*
 * Copyright 2016 ZomboDB, LLC
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcdi.zombodb.query_parser.Utils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;

import java.util.List;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbVacuumSupportAction extends BaseRestHandler {

    @Inject
    protected ZombodbVacuumSupportAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_zdbvacsup", this);
        controller.registerHandler(POST, "/{index}/_zdbvacsup", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        String input = request.param("xids");
        if (input == null)
            input = request.content().toUtf8();
        List<Long> xids = new ObjectMapper().readValue(input, new TypeReference<List<Long>>(){});

        FiltersAggregationBuilder fab = new FiltersAggregationBuilder("xids");
        for (Long xid : xids)
            fab.filter(xid.toString(), termFilter("_xid", xid.toString()));

        SearchRequestBuilder srb = new SearchRequestBuilder(client);
        srb.setIndices(request.param("index"));
        srb.setTypes("data");
        srb.setSize(0);
        srb.addAggregation(fab);

        SearchResponse response = client.search(srb.request()).get();

        xids.clear();
        Filters agg = response.getAggregations().get("xids");
        for (MultiBucketsAggregation.Bucket bucket : agg.getBuckets()) {
            if (bucket.getDocCount() == 0)
                xids.add(Long.valueOf(bucket.getKey()));
        }

        byte[] bytes = new byte[4 + (xids.size() * 8)];
        int offset = 0;

        offset += Utils.encodeInteger(xids.size(), bytes, offset);
        for (Long xid : xids) {
            offset += Utils.encodeLong(xid, bytes, offset);
        }

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", bytes));
    }
}
