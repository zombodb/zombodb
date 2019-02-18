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
import llc.zombodb.fast_terms.collectors.NumberCollector;
import llc.zombodb.fast_terms.collectors.StringCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
                ThreadPool.Names.GENERIC
        );
        this.indicesService = indicesService;
    }


    @Override
    protected FastTermsResponse newResponse(FastTermsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        FastTermsResponse.DataType dataType = FastTermsResponse.DataType.NONE;
        List<ShardFastTermsResponse> successful = new ArrayList<>();

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);

            if (!(shardResponse instanceof ShardFastTermsResponse)) {
                // we have an error of some kind to deal with
                if (shardResponse == null)
                    shardResponse = new DefaultShardOperationFailedException(request.indices()[0], i, new ElasticsearchException("No Response for element [" + i + "] in [" + request.indices()[0] + "]"));

                DefaultShardOperationFailedException failure = shardResponse instanceof DefaultShardOperationFailedException ?
                        (DefaultShardOperationFailedException) shardResponse :
                        shardResponse instanceof ElasticsearchException ?
                                new DefaultShardOperationFailedException((ElasticsearchException) shardResponse) :
                                new DefaultShardOperationFailedException(request.indices()[0], i, new ElasticsearchException("Unknown failure for element [" + i + "], response=" + shardResponse));

                logger.error(failure.toString(), failure);
                failedShards++;

                if (shardFailures == null) {
                    shardFailures = new LinkedList<>();
                }

                shardFailures.add(failure);
            } else {
                // everything worked

                successfulShards++;
                ShardFastTermsResponse resp = (ShardFastTermsResponse) shardResponse;

                if (resp.getDataCount() == 0) {
                    continue;   // this one is empty and we don't need it
                }

                if (dataType == FastTermsResponse.DataType.NONE)
                    dataType = resp.getDataType();
                else if (dataType != resp.getDataType())
                    throw new RuntimeException("Data Types from shards don't match");

                successful.add(resp);
            }
        }

        FastTermsResponse response = new FastTermsResponse(request.indices()[0], successful.size(), successfulShards, failedShards, shardFailures, dataType);
        for (int i = 0; i < successful.size(); i++) {
            ShardFastTermsResponse shardResponse = successful.get(i);
            response.addData(i, shardResponse.getData());
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

        if (indexShard.state() != IndexShardState.STARTED)
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());

        if (request.getRequest().sourceShardId() == -1 || shardId == request.getRequest().sourceShardId()) {
            String fieldname = request.getRequest().fieldname();
            FastTermsResponse.DataType type;
            FastTermsCollector collector;

            try (Engine.Searcher engine = indexShard.acquireSearcher("fastterms")) {
                IndexSearcher searcher = new IndexSearcher(engine.reader());
                QueryShardContext context = indicesService.indexServiceSafe(index).newQueryShardContext(shardId, engine.reader(), System::currentTimeMillis);
                Query query = context.toQuery(request.getRequest().query()).query();
                MappedFieldType mappedFieldType = context.fieldMapper(fieldname);

                if (mappedFieldType == null)
                    throw new RuntimeException(fieldname + " does not exist in " + index.getName());

                switch (mappedFieldType.typeName()) {
                    case "integer":
                        type = FastTermsResponse.DataType.INT;
                        collector = new NumberCollector(fieldname);
                        break;
                    case "long":
                        type = FastTermsResponse.DataType.LONG;
                        collector = new NumberCollector(fieldname);
                        break;
                    case "keyword":
                        type = FastTermsResponse.DataType.STRING;
                        collector = new StringCollector(fieldname);
                        break;
                    default:
                        throw new RuntimeException("Unrecognized data type: " + context.fieldMapper(fieldname).typeName());
                }

                searcher.search(query, collector);
            } catch (Throwable t) {
                logger.error("Error while executing FastTerms", t);
                throw t;
            }

            return new ShardFastTermsResponse(request.shardId(), type, collector);
        } else {
            return new ShardFastTermsResponse(request.shardId());
        }
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState, FastTermsRequest request, String[] concreteIndices) {
        if (request.sourceShardId() != -1)
            return new GroupShardsIterator<>(Collections.singletonList(clusterService.operationRouting()
                    .getShards(clusterService.state(), request.indices()[0], request.sourceShardId(), null)));
        else
            return clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, false);
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
