/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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
package com.tcdi.zombodb;

import com.tcdi.zombodb.postgres.*;
import com.tcdi.zombodb.query.ZomboDBVisibilityQueryParser;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.xbib.elasticsearch.action.termlist.TermlistAction;
import org.xbib.elasticsearch.action.termlist.TransportTermlistAction;
import org.xbib.elasticsearch.rest.action.termlist.RestTermlistAction;

public class ZombodbPlugin extends AbstractPlugin {

    @Inject
    public ZombodbPlugin(Settings settings) {
        // noop
    }

    public void onModule(RestModule module) {
        module.addRestAction(PostgresTIDResponseAction.class);
        module.addRestAction(PostgresAggregationAction.class);
        module.addRestAction(PostgresCountAction.class);
        module.addRestAction(PostgresMappingAction.class);
        module.addRestAction(ZombodbQueryAction.class);
        module.addRestAction(ZombodbDocumentHighlighterAction.class);
        module.addRestAction(ZombodbMultiSearchAction.class);
        module.addRestAction(RestTermlistAction.class);
        module.addRestAction(ZombodbBulkAction.class);
        module.addRestAction(ZombodbCommitXIDAction.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(TermlistAction.INSTANCE, TransportTermlistAction.class);
    }

    public void onModule(IndicesQueriesModule module) {
        module.addQuery(ZomboDBVisibilityQueryParser.class);
    }

    @Override
    public String name() {
        return "Zombodb";
    }

    @Override
    public String description() {
        return "ZomboDB support plugin";
    }
}
