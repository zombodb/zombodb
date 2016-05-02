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

import com.tcdi.zombodb.query_parser.QueryRewriter;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Created by e_ridge on 10/16/14.
 */
public class ZombodbQueryAction extends BaseRestHandler {
    @Inject
    public ZombodbQueryAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_zdbquery", this);
        controller.registerHandler(POST, "/{index}/_zdbquery", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        String query = request.param("q");
        BytesRestResponse response;

        if (query == null)
            query = request.content().toUtf8();

        if (query != null && query.trim().length() > 0) {
            try {
                QueryRewriter qr;
                String json;

                qr = new QueryRewriter(client, request.param("index"), request.param("preference"), query, false, true);
                json = qr.rewriteQuery().toString();

                response = new BytesRestResponse(RestStatus.OK, "application/json", json);
            } catch (Exception e) {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("error", e.toString());
                builder.endObject();
                response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
            }
        } else {
            response = new BytesRestResponse(RestStatus.OK, "application/json", "{ \"match_all\": {} }");
        }

        channel.sendResponse(response);
    }
}
