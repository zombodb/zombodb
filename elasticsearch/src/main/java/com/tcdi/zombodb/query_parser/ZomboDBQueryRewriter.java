/*
 * Copyright 2016 ZomboDB, LLC
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
package com.tcdi.zombodb.query_parser;

import org.elasticsearch.client.Client;

/**
 * ZomboDB's stock {@link QueryRewriter} that resolves joins during construction
 */
public class ZomboDBQueryRewriter extends QueryRewriter {

    public ZomboDBQueryRewriter(Client client, String indexName, String searchPreference, String input, boolean doFullFieldDataLookup, boolean canDoSingleIndex) {
        super(client, indexName, input, searchPreference, doFullFieldDataLookup, canDoSingleIndex);
    }

    @Override
    protected void performOptimizations(Client client) {
        super.performOptimizations(client);
        new ExpansionOptimizer(this, tree, metadataManager, client, searchPreference, doFullFieldDataLookup).optimize();
    }
}
