/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2017 ZomboDB, LLC
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
import com.tcdi.zombodb.query.ZomboDBVisibilityQueryBuilder;
import com.tcdi.zombodb.query.ZomboDBVisibilityQueryParser;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.xbib.elasticsearch.action.termlist.TermlistAction;
import org.xbib.elasticsearch.action.termlist.TransportTermlistAction;
import org.xbib.elasticsearch.rest.action.termlist.RestTermlistAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ZombodbPlugin extends Plugin implements ActionPlugin, SearchPlugin {

    private ClusterService clusterService;

    public ZombodbPlugin() {
        // noop
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry) {
        this.clusterService = clusterService;
        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService, xContentRegistry);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, java.util.function.Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new PostgresTIDResponseAction(settings, restController),
                new PostgresAggregationAction(settings, restController),
                new PostgresCountAction(settings, restController),
                new PostgresMappingAction(settings, restController),
                new ZombodbQueryAction(settings, restController),
                new ZombodbDocumentHighlighterAction(settings, restController),
                new ZombodbMultiSearchAction(settings, restController),
                new RestTermlistAction(settings, restController),
                new ZombodbBulkAction(settings, restController, clusterService),
                new ZombodbCommitXIDAction(settings, restController, clusterService),
                new ZombodbDeleteTuplesAction(settings, restController, clusterService),
                new ZombodbGetXidVacuumCandidatesAction(settings, restController),
                new ZombodbVacuumCleanupAction(settings, restController, clusterService)

        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
                new ActionHandler<>(TermlistAction.INSTANCE, TransportTermlistAction.class)
        );
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
                new QuerySpec<>("visibility", new ZomboDBVisibilityQueryParser(), new ZomboDBVisibilityQueryParser())
        );
    }
}
