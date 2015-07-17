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
package com.tcdi.zombodb.entity;


import com.tcdi.entityextractor.EntityExtractor;
import com.tcdi.entityextractor.OpenNLPEntityExtractor;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class EntityExtractorAction extends BaseRestHandler {
    private static AtomicInteger COUNTER = new AtomicInteger(0);

    @Inject
    public EntityExtractorAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_entities", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, RestChannel channel, Client client) throws Exception {
        if (COUNTER.incrementAndGet() % 100 == 0)
            logger.info("Extracted entities for " + COUNTER.get() + " requests");

        channel.sendResponse(new RestResponse() {
            @Override
            public String contentType() {
                return "application/json";
            }

            @Override
            public boolean contentThreadSafe() {
                return true;
            }

            @Override
            public BytesReference content() {
                final byte[] data = request.content().hasArray() ? request.content().array() : request.content().toBytes();
                try {
                    final ObjectMapper mapper = new ObjectMapper();
                    final Map<String, Object> postData = mapper.readValue(data, Map.class);

                    String text = (String) postData.get("text");
                    if (text == null || text.length() == 0)
                        return new BytesArray("{}");

                    Map<EntityExtractor.EntityType, Set<String>> categories = new OpenNLPEntityExtractor().extractEntities(text);
                    return new BytesArray(mapper.writeValueAsBytes(categories));
                } catch (Exception e) {
                    logger.error("Problem serializing entities: " + new String(data), e);
                    return new BytesArray("{}");
                }
            }

            @Override
            public RestStatus status() {
                return RestStatus.OK;
            }
        });
    }
}
