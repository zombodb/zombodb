/**
 * Portions Copyright (C) 2011-2015 Jörg Prante
 * Portions Copyright (C) 2016 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.xbib.elasticsearch.action.termlist;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request to get termlists of one or more indices.
 */
public class TermlistRequestBuilder extends BroadcastOperationRequestBuilder<TermlistRequest, TermlistResponse, TermlistRequestBuilder> {

    public TermlistRequestBuilder(ElasticsearchClient client, TermlistAction action) {
        super(client, action, new TermlistRequest());
    }

    public TermlistRequestBuilder setField(String field) {
        request.setFieldname(field);
        return this;
    }

    public TermlistRequestBuilder setTerm(String term) {
        request.setPrefix(term);
        return this;
    }

    public TermlistRequestBuilder setSize(Integer size) {
        request.setSize(size);
        return this;
    }

    public TermlistRequestBuilder setStartAt(String term) {
        request.setStartAt(term);
        return this;
    }

    @Override
    public void execute(ActionListener<TermlistResponse> listener) {
        client.execute(TermlistAction.INSTANCE, request, listener);
    }
}
