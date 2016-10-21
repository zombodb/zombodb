/*
 * Copyright 2015-2016 ZomboDB, LLC
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

import com.tcdi.zombodb.query_parser.utils.Utils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class ZombodbVacuumSupport extends BaseRestHandler {

    @Inject
    protected ZombodbVacuumSupport(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(GET, "/{index}/_zdbvacsup", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        SearchRequestBuilder builder = new SearchRequestBuilder(client)
                .setIndices(request.param("index"))
                .setTypes("state")
                .setSize(Integer.MAX_VALUE);

        SearchResponse response = client.search(builder.request()).actionGet();
        int many = response.getHits().getHits().length;
        byte[] buffer = new byte[1 + 4 + 6*many];
        int offset = 0;

        buffer[0] = 0;
        offset++;

        offset += Utils.encodeInteger(many, buffer, offset);
        for (SearchHit hit : response.getHits().getHits()) {
            String id = hit.id();
            String[] parts = id.split("[-]");

            offset += Utils.encodeInteger(Integer.parseInt(parts[0]), buffer, offset);
            offset += Utils.encodeCharacter((char) Integer.parseInt(parts[1]), buffer, offset);
        }

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", buffer));
    }
}
