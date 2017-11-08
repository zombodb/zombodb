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
package llc.zombodb.rest.vacuum;

import llc.zombodb.query_parser.utils.Utils;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class ZomboDBGetXidVacuumCandidatesAction extends BaseRestHandler {

    @Inject
    public ZomboDBGetXidVacuumCandidatesAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/{index}/_zdbxidvacuumcandidates", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");

        // the transaction ids we can consider for vacuuming are simply
        // all the _zdb_xid values in the "aborted" type
        // Some of these xids may still be in-progress, but that's okay
        // because Postgres will decide for us which ones are really aborted
        SearchRequestBuilder search = SearchAction.INSTANCE.newRequestBuilder(client)
                .setIndices(index)
                .setTypes("aborted")
                .setScroll(TimeValue.timeValueMinutes(10))
                .setSize(10000)
                .addFieldDataField("_zdb_xid");

        Set<Long> xids = new HashSet<>();
        int total = 0, cnt = 0;
        SearchResponse response = null;
        while (true) {
            if (response == null) {
                response = client.execute(SearchAction.INSTANCE, search.request()).actionGet();
                total = (int) response.getHits().getTotalHits();
            } else {
                response = client.execute(SearchScrollAction.INSTANCE,
                        SearchScrollAction.INSTANCE.newRequestBuilder(client)
                                .setScrollId(response.getScrollId())
                                .setScroll(TimeValue.timeValueMinutes(10))
                                .request()).actionGet();
            }

            for (SearchHit hit : response.getHits()) {
                Number xid = hit.field("_zdb_xid").value();
                xids.add(xid.longValue());

                cnt++;
            }

            if (cnt == total)
                break;
        }

        byte[] bytes = new byte[1 + 8 + 8 * xids.size()];
        int offset = 1; // first byte is null to indicate binary response
        offset += Utils.encodeLong(xids.size(), bytes, offset);
        for (Long xid : xids) {
            offset += Utils.encodeLong(xid, bytes, offset);
        }

        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/data", bytes));
    }
}
