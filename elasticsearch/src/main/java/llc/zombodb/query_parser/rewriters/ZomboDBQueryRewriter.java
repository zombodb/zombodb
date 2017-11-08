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
package llc.zombodb.query_parser.rewriters;

import llc.zombodb.query_parser.optimizers.ExpansionOptimizer;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

/**
 * ZomboDB's stock {@link QueryRewriter} that resolves joins during construction
 */
public class ZomboDBQueryRewriter extends QueryRewriter {

    public ZomboDBQueryRewriter(Client client, String indexName, NamedXContentRegistry contentRegistry, String searchPreference, String input, boolean doFullFieldDataLookup, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel) {
        super(client, indexName, contentRegistry, input, searchPreference, doFullFieldDataLookup, canDoSingleIndex, needVisibilityOnTopLevel);
    }

    @Override
    protected void performOptimizations(Client client) {
        super.performOptimizations(client);
        new ExpansionOptimizer(this, tree, metadataManager, client, searchPreference, doFullFieldDataLookup).optimize();
    }
}
