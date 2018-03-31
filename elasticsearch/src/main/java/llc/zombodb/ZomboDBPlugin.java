/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2018 ZomboDB, LLC
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
package llc.zombodb;

import llc.zombodb.cross_join.CrossJoinQueryBuilder;
import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.TransportFastTermsAction;
import llc.zombodb.rest.admin.ZomboDBMappingAction;
import llc.zombodb.rest.admin.ZomboDBQueryAction;
import llc.zombodb.rest.highlight.ZomboDBDocumentHighlighterAction;
import llc.zombodb.rest.search.ZomboDBAggregationAction;
import llc.zombodb.rest.search.ZomboDBCountAction;
import llc.zombodb.rest.search.ZomboDBMultiSearchAction;
import llc.zombodb.rest.search.ZomboDBTIDResponseAction;
import llc.zombodb.rest.vacuum.ZomboDBGetXidVacuumCandidatesAction;
import llc.zombodb.rest.vacuum.ZomboDBVacuumCleanupAction;
import llc.zombodb.rest.xact.ZomboDBBulkAction;
import llc.zombodb.rest.xact.ZomboDBCommitXIDAction;
import llc.zombodb.rest.xact.ZomboDBDeleteTuplesAction;
import llc.zombodb.visibility_query.ZomboDBVisibilityQueryBuilder;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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
import java.util.List;

public class ZomboDBPlugin extends Plugin implements ActionPlugin, SearchPlugin {

    private ClusterService clusterService;

    public ZomboDBPlugin() {
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
                new ZomboDBTIDResponseAction(settings, restController),
                new ZomboDBAggregationAction(settings, restController),
                new ZomboDBCountAction(settings, restController),
                new ZomboDBMappingAction(settings, restController),
                new ZomboDBQueryAction(settings, restController),
                new ZomboDBDocumentHighlighterAction(settings, restController),
                new ZomboDBMultiSearchAction(settings, restController),
                new RestTermlistAction(settings, restController),
                new ZomboDBBulkAction(settings, restController, clusterService),
                new ZomboDBCommitXIDAction(settings, restController, clusterService),
                new ZomboDBDeleteTuplesAction(settings, restController, clusterService),
                new ZomboDBGetXidVacuumCandidatesAction(settings, restController),
                new ZomboDBVacuumCleanupAction(settings, restController, clusterService)

        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(TermlistAction.INSTANCE, TransportTermlistAction.class),
                new ActionHandler<>(FastTermsAction.INSTANCE, TransportFastTermsAction.class)
        );
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
                new QuerySpec<>(ZomboDBVisibilityQueryBuilder.NAME, ZomboDBVisibilityQueryBuilder::new, ZomboDBVisibilityQueryBuilder::fromXContent),
                new QuerySpec<>(CrossJoinQueryBuilder.NAME, CrossJoinQueryBuilder::new, CrossJoinQueryBuilder::fromXContent)
        );
    }
}
