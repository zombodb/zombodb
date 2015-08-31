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
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * @author e_ridge
 */
public class PostgresMappingAction extends BaseRestHandler {

    @Inject
    public PostgresMappingAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_pgmapping/{fieldname}", this);
        controller.registerHandler(POST, "/{index}/_pgmapping/{fieldname}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        BytesRestResponse response;

        QueryRewriter rewriter = new QueryRewriter(client, request, request.content().toUtf8(), false, true);
        rewriter.rewriteQuery();
        Map<String, ?> properties = rewriter.describedNestedObject(request.param("fieldname"));

        response = new BytesRestResponse(RestStatus.OK, "application/json", JsonXContent.contentBuilder().map(properties).bytes());
        channel.sendResponse(response);
    }
}
