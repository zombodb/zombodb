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

import com.tcdi.zombodb.highlight.AnalyzedField;
import com.tcdi.zombodb.highlight.DocumentHighlighter;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * @author e_ridge
 */
public class ZombodbDocumentHighlighterAction extends BaseRestHandler {

    @Inject
    public ZombodbDocumentHighlighterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_zdbhighlighter", this);
        controller.registerHandler(POST, "/{index}/_zdbhighlighter", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        BytesRestResponse response;
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> input;
        String queryString;
        String primaryKeyFieldname;
        List<Map<String, Object>> documents;

        input = om.readValue(request.content().streamInput(), Map.class);
        queryString = input.get("query").toString();
        primaryKeyFieldname = input.get("primary_key").toString();
        documents = (List<Map<String, Object>>) input.get("documents");

        try {
            List<AnalyzedField.Token> tokens = new ArrayList<>();

            for (Map<String, Object> document : documents) {
                DocumentHighlighter highlighter = new DocumentHighlighter(client, request.param("index"), primaryKeyFieldname, document, queryString);
                tokens.addAll(highlighter.highlight());
            }

            response = new BytesRestResponse(RestStatus.OK, "application/text", new ObjectMapper().writeValueAsString(tokens));
            channel.sendResponse(response);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
