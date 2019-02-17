/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2019 ZomboDB, LLC
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
package llc.zombodb.rest.highlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import llc.zombodb.highlight.AnalyzedField;
import llc.zombodb.highlight.DocumentHighlighter;
import llc.zombodb.query_parser.utils.Utils;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBDocumentHighlighterAction extends BaseRestHandler {

    @Inject
    public ZomboDBDocumentHighlighterAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/_zdbhighlighter", this);
        controller.registerHandler(POST, "/{index}/_zdbhighlighter", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> input;
        String queryString;
        String primaryKeyFieldname;
        String fieldLists;
        List<Map<String, Object>> documents;

        input = Utils.jsonToObject(request.content().streamInput(), Map.class);
        queryString = input.get("query").toString();
        fieldLists = input.containsKey("field_lists") ? input.get("field_lists").toString() : null;
        primaryKeyFieldname = input.get("primary_key").toString();
        documents = (List<Map<String, Object>>) input.get("documents");

        if (fieldLists != null)
            queryString = "#field_lists(" + fieldLists + ") " + queryString;

        List<AnalyzedField.Token> tokens = new ArrayList<>();

        for (Map<String, Object> document : documents) {
            DocumentHighlighter highlighter = new DocumentHighlighter(client, request.param("index"), primaryKeyFieldname, document, queryString);
            tokens.addAll(highlighter.highlight());
        }

        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/text", Utils.objectToJson(tokens)));
    }
}
