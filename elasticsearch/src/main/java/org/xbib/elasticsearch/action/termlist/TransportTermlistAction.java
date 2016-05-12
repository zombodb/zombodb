/**
 Portions Copyright (C) 2011-2015 Jörg Prante
 Portions Copyright (C) 2016 ZomboDB, LLC

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 specific language governing permissions and limitations under the License.
 */
package org.xbib.elasticsearch.action.termlist;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Termlist index/indices action.
 */
public class TransportTermlistAction
        extends TransportBroadcastOperationAction<TermlistRequest, TermlistResponse, ShardTermlistRequest, ShardTermlistResponse> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportTermlistAction.class.getName());

    private final IndicesService indicesService;

    @Inject
    public TransportTermlistAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   TransportService transportService,
                                   IndicesService indicesService,
                                   ActionFilters actionFilters) {
        super(settings, TermlistAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected TermlistRequest newRequest() {
        return new TermlistRequest();
    }

    @Override
    protected TermlistResponse newResponse(TermlistRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        int numdocs = 0;
        Map<String, TermInfo> terms = new TreeMap<>();

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse instanceof BroadcastShardOperationFailedException) {
                BroadcastShardOperationFailedException e = (BroadcastShardOperationFailedException)shardResponse;
                logger.error(e.getMessage(), e);
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new LinkedList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException(e));
            } else {
                if (shardResponse instanceof ShardTermlistResponse) {
                    successfulShards++;
                    ShardTermlistResponse resp = (ShardTermlistResponse) shardResponse;
                    numdocs += resp.getNumDocs();

                    for (TermInfo ti : resp.getTermList()) {
                        String term = ti.getTerm();
                        TermInfo existing = terms.get(term);
                        if (existing != null) {
                            existing.setDocFreq(existing.getDocFreq() + ti.getDocFreq());
                            existing.setTotalFreq(existing.getTotalFreq() + ti.getTotalFreq());
                        } else {
                            terms.put(term, ti);
                        }
                    }
                }
            }
        }

        List<TermInfo> values = new ArrayList<>(terms.size());
        int cnt = 0;
        for (TermInfo ti : terms.values()) {
            values.add(ti);
            if (++cnt == request.getSize())
                break;
        }

        return new TermlistResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, numdocs, values);
    }

    @Override
    protected ShardTermlistRequest newShardRequest() {
        return new ShardTermlistRequest();
    }

    @Override
    protected ShardTermlistRequest newShardRequest(int numShards, ShardRouting shard, TermlistRequest request) {
        return new ShardTermlistRequest(shard.getIndex(), shard.shardId(), request);
    }

    @Override
    protected ShardTermlistResponse newShardResponse() {
        return new ShardTermlistResponse();
    }

    /**
     * The termlist request works against primary shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, TermlistRequest request, String[] concreteIndices) {
        return clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermlistRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermlistRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardTermlistResponse shardOperation(ShardTermlistRequest request) throws ElasticsearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.getIndex()).shardSafe(request.shardId().id());
        try (Engine.Searcher searcher = indexShard.engine().acquireSearcher("zdbtermlist")) {
            String startAt = request.getRequest().getStartAt();
            BytesRef prefix = request.getRequest().hasUsableTermPrefix() ? new BytesRef(Strings.toUTF8Bytes(startAt == null ? request.getRequest().getPrefix() : startAt)) : null;
            IndexReader reader = searcher.reader();
            Fields fields = MultiFields.getFields(reader);
            List<TermInfo> termsList = new LinkedList<>();

            if (fields != null) {
                Terms terms = fields.terms(request.getRequest().getFieldname());

                // Returns the number of documents that have at least one
                if (terms != null) {
                    TermsEnum.SeekStatus status;
                    TermsEnum termsEnum;
                    BytesRef term;

                    // start iterating terms and...
                    termsEnum = terms.iterator(null);
                    if (prefix != null) {
                        // seek to our term prefix (if we have one)
                        status = termsEnum.seekCeil(prefix);
                        term = termsEnum.term();
                    } else {
                        // just start at the top
                        status = TermsEnum.SeekStatus.FOUND;
                        term = termsEnum.next();
                    }

                    if (status != TermsEnum.SeekStatus.END) {
                        do {
                            String text = Term.toString(term);

                            if (prefix != null && !text.startsWith(request.getRequest().getPrefix())) {
                                // we've moved past the index terms that match our prefix
                                break;
                            }

                            if (!text.equals(startAt))
                                termsList.add(new TermInfo(text, termsEnum.docFreq(), termsEnum.totalTermFreq()));

                            if (termsList.size() == request.getRequest().getSize()) {
                                // we've collected as many terms as we should (for this shard)
                                break;
                            }

                        } while ((term = termsEnum.next()) != null);
                    }
                }
            }
            return new ShardTermlistResponse(request.getIndex(), request.shardId(), reader.numDocs(), termsList);
        } catch (Throwable ex) {
            logger.error(ex.getMessage(), ex);
            throw new ElasticsearchException(ex.getMessage(), ex);
        }
    }
}