/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2018 ZomboDB, LLC
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
package llc.zombodb.rest.admin;

import llc.zombodb.query_parser.rewriters.QueryRewriter;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBMappingAction extends BaseRestHandler {

    @Inject
    public ZomboDBMappingAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/{index}/_pgmapping/{fieldname}", this);
        controller.registerHandler(POST, "/{index}/_pgmapping/{fieldname}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        QueryRewriter rewriter = QueryRewriter.Factory.create(request, client, request.param("index"), request.content().utf8ToString(), false, false);
        rewriter.rewriteQuery();
        Map<String, ?> properties = rewriter.describedNestedObject(request.param("fieldname"));

        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", JsonXContent.contentBuilder().map(properties).bytes()));
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }

}
