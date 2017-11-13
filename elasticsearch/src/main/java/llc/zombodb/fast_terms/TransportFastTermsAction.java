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

import llc.zombodb.fast_terms.collectors.FastTermsCollector;
import llc.zombodb.fast_terms.collectors.IntCollector;
import llc.zombodb.fast_terms.collectors.LongCollector;
import llc.zombodb.fast_terms.collectors.StringCollector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportFastTermsAction extends TransportBroadcastAction<FastTermsRequest, FastTermsResponse, ShardFastTermsRequest, ShardFastTermsResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFastTermsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService) {
        super(
                settings,
                FastTermsAction.NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                FastTermsRequest::new,
                ShardFastTermsRequest::new,
                ThreadPool.Names.GENERIC    // the SEARCH thread pool can deadlock as it's of a fixed size, GENERIC is not
        );
        this.indicesService = indicesService;
    }


    @Override
    protected FastTermsResponse newResponse(FastTermsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        FastTermsResponse.DataType dataType = null;
        List<Object> datas = new ArrayList<>();
        List<Integer> counts = new ArrayList<>();

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse instanceof BroadcastShardOperationFailedException) {
                BroadcastShardOperationFailedException e = (BroadcastShardOperationFailedException) shardResponse;
                logger.error(e.getMessage(), e);
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new LinkedList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException(e));
            } else {
                if (shardResponse instanceof ShardFastTermsResponse) {
                    successfulShards++;
                    ShardFastTermsResponse resp = (ShardFastTermsResponse) shardResponse;

                    if (dataType == null)
                        dataType = resp.getDataType();
                    else if (dataType != resp.getDataType())
                        throw new RuntimeException("Data Types from shards don't match");

                    datas.add(resp.getData());
                    counts.add(resp.getDataCount());
                }
            }
        }

        FastTermsResponse response = new FastTermsResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, dataType);
        for (int i = 0; i < datas.size(); i++) {
            response.addData(i, datas.get(i), counts.get(i));
        }
        return response;
    }

    @Override
    protected ShardFastTermsRequest newShardRequest(int numShards, ShardRouting shard, FastTermsRequest request) {
        return new ShardFastTermsRequest(shard.getIndexName(), shard.shardId(), request);
    }

    @Override
    protected ShardFastTermsResponse newShardResponse() {
        return new ShardFastTermsResponse();
    }

    @Override
    protected ShardFastTermsResponse shardOperation(ShardFastTermsRequest request) throws IOException {
        Index index = request.shardId().getIndex();
        int shardId = request.shardId().id();
        IndexShard indexShard = indicesService.indexServiceSafe(index).getShard(shardId);
        String fieldname = request.getRequest().fieldname();
        FastTermsResponse.DataType type;
        FastTermsCollector collector;

        try (Engine.Searcher engine = indexShard.acquireSearcher("fastterms")) {
            IndexSearcher searcher = new IndexSearcher(engine.reader());
            QueryShardContext context = indicesService.indexServiceSafe(index).newQueryShardContext(shardId, engine.reader(), System::currentTimeMillis);
            Query query = request.getRequest().query().toQuery(context);

            switch (context.fieldMapper(fieldname).typeName()) {
                case "integer":
                    type = FastTermsResponse.DataType.INT;
                    collector = new IntCollector(fieldname);
                    break;
                case "long":
                    type = FastTermsResponse.DataType.LONG;
                    collector = new LongCollector(fieldname);
                    break;
                case "keyword":
                    type = FastTermsResponse.DataType.STRING;
                    collector = new StringCollector(fieldname);
                    break;
                default:
                    throw new RuntimeException("Unrecognized data type: " + context.fieldMapper(fieldname).typeName());
            }

            searcher.search(new ConstantScoreQuery(query.rewrite(engine.reader())), collector);
        }

        return new ShardFastTermsResponse(request.shardId(), type, collector.getData(), collector.getDataCount());
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState, FastTermsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FastTermsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FastTermsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
