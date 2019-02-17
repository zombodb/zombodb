/**
 * Portions Copyright (C) 2011-2015 Jörg Prante
 * Portions Copyright (C) 2017 ZomboDB, LLC
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.xbib.elasticsearch.rest.action.termlist;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.xbib.elasticsearch.action.termlist.TermInfo;
import org.xbib.elasticsearch.action.termlist.TermlistAction;
import org.xbib.elasticsearch.action.termlist.TermlistRequest;
import org.xbib.elasticsearch.action.termlist.TermlistResponse;

import llc.zombodb.query_parser.utils.Utils;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.RestActions.buildBroadcastShardsHeader;

public class RestTermlistAction extends BaseRestHandler {

    public static class TermListDescriptor {
        public String fieldname;
        public String prefix;
        public String startAt;
        public int size;
    }

    @Inject
    public RestTermlistAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/{index}/_zdbtermlist", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final TermListDescriptor descriptor = Utils.jsonToObject(request.content().utf8ToString(), TermListDescriptor.class);
        TermlistRequest termlistRequest = new TermlistRequest(Strings.splitStringByCommaToArray(request.param("index")));
        termlistRequest.setFieldname(descriptor.fieldname);
        termlistRequest.setPrefix(descriptor.prefix);
        termlistRequest.setStartAt(descriptor.startAt);
        termlistRequest.setSize(descriptor.size);
        final long t0 = System.nanoTime();
        final long start = System.currentTimeMillis();
        return channel -> client.execute(TermlistAction.INSTANCE, termlistRequest, new RestBuilderListener<TermlistResponse>(channel) {
            @Override
            public RestResponse buildResponse(TermlistResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                buildBroadcastShardsHeader(builder, request, response);
                builder.field("took", (System.nanoTime() - t0) / 1000000);
                builder.field("numdocs", response.getNumDocs());
                builder.field("numterms", response.getTermlist().size());
                builder.startArray("terms");

                for (TermInfo tl : response.getTermlist()) {
                    builder.startObject();
                    tl.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                long end = System.currentTimeMillis();
                logger.info("Retrieved " + response.getTermlist().size() + " terms from " + request.param("index") + "." + descriptor.fieldname + " in " + ((end - start) / 1000D) + " seconds");
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
