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
package llc.zombodb.fast_terms;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class FastTermsAction extends Action<FastTermsRequest, FastTermsResponse, FastTermsRequestBuilder> {

    public static final FastTermsAction INSTANCE = new FastTermsAction();

    public static final String NAME = "indices/fastterms";

    private FastTermsAction() {
        super(NAME);
    }

    @Override
    public FastTermsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new FastTermsRequestBuilder(client, this);
    }

    @Override
    public FastTermsResponse newResponse() {
        return new FastTermsResponse();
    }
}
