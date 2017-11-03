/*
 * Copyright 2017 ZomboDB, LLC
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

import com.tcdi.zombodb.query_parser.rewriters.QueryRewriter;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbQueryAction extends BaseRestHandler {
    @Inject
    public ZombodbQueryAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/_zdbquery", this);
        controller.registerHandler(POST, "/{index}/_zdbquery", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String query = request.param("q");
        BytesRestResponse response;

        if (query == null)
            query = request.content().utf8ToString();

        if (query != null && query.trim().length() > 0) {
            try {
                QueryRewriter qr;
                String json;

                qr = QueryRewriter.Factory.create(request, client, request.param("index"), request.param("preference"), query, true, false, false);
                json = qr.rewriteQuery().toString();

                response = new BytesRestResponse(RestStatus.OK, "application/json", json);
            } catch (Exception e) {
                logger.error("Error building query", e);
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
                builder.startObject();
                builder.field("error", ExceptionsHelper.stackTrace(e));
                builder.endObject();
                response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
            }
        } else {
            response = new BytesRestResponse(RestStatus.OK, "application/json", "{ \"match_all\": {} }");
        }

        BytesRestResponse finalResponse = response;
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, finalResponse.contentType(), finalResponse.content()));
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
