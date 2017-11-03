/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2017 ZomboDB, LLC
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.tcdi.zombodb.highlight.AnalyzedField;
import com.tcdi.zombodb.highlight.DocumentHighlighter;
import com.tcdi.zombodb.query_parser.utils.Utils;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbDocumentHighlighterAction extends BaseRestHandler {

    @Inject
    public ZombodbDocumentHighlighterAction(Settings settings, RestController controller) {
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
