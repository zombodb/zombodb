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

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

public class TermlistAction extends ClientAction<TermlistRequest, TermlistResponse, TermlistRequestBuilder> {

    public static final TermlistAction INSTANCE = new TermlistAction();

    public static final String NAME = "indices/zdbtermlist";

    private TermlistAction() {
        super(NAME);
    }

    @Override
    public TermlistResponse newResponse() {
        return new TermlistResponse();
    }

    @Override
    public TermlistRequestBuilder newRequestBuilder(Client client) {
        return new TermlistRequestBuilder(client);
    }
}
