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
package llc.zombodb.rest.admin;

import llc.zombodb.rest.QueryAndIndexPair;
import llc.zombodb.rest.search.ZomboDBTIDResponseAction;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBQueryAction extends BaseRestHandler {

    @Inject
    public ZomboDBQueryAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/{index}/_zdbquery", this);
        controller.registerHandler(POST, "/{index}/_zdbquery", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String preference = request.param("preference");
        boolean profile = request.paramAsBoolean("profile", false);

        try {
            QueryAndIndexPair queryAndIndex = ZomboDBTIDResponseAction.buildJsonQueryFromRequestContent(client, request, true, true);

            if (profile) {
                return channel -> SearchAction.INSTANCE.newRequestBuilder(client)
                        .setProfile(true)
                        .setIndices(index)
                        .setTypes("data")
                        .setSize(0)
                        .setPreference(preference)
                        .setQuery(queryAndIndex.getQueryBuilder()).execute(new RestStatusToXContentListener<>(channel));
            } else {
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).prettyPrint();
                queryAndIndex.getQueryBuilder().toXContent(builder, null);
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        } catch (Exception e) {
            logger.error("Error building query", e);
            XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).prettyPrint();
            builder.startObject();
            builder.field("error", ExceptionsHelper.stackTrace(e));
            builder.endObject();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
