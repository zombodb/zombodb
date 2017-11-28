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
package llc.zombodb.cross_join;

import llc.zombodb.ZomboDBPlugin;
import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.FastTermsResponse;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

class CrossJoinQuery extends Query {

    private static final Map<String, TransportClient> CLIENTS = new ConcurrentHashMap<>();
    private static final Cache<String, FastTermsResponse> RESPONSE_CACHE = CacheBuilder.<String, FastTermsResponse>builder()
            .setExpireAfterAccess(TimeValue.timeValueMinutes(1))
            .setExpireAfterWrite(TimeValue.timeValueMinutes(1))
            .build();

    private final String cacheKey;
    private final String clusterName;
    private final String host;
    private final int port;
    private final String index;
    private final String type;
    private final String leftFieldname;
    private final String rightFieldname;
    private final QueryBuilder query;
    private final String fieldType;
    private final int thisShardId;
    private final boolean canOptimizeJoins;

    private FastTermsResponse fastTerms;

    public CrossJoinQuery(String clusterName, String host, int port, String index, String type, String leftFieldname, String rightFieldname, QueryBuilder query, boolean canOptimizeJoins, String fieldType, int thisShardId) {
        this.cacheKey = (clusterName + host + port + index + type + leftFieldname + rightFieldname + query + (canOptimizeJoins ? thisShardId : -1)).intern();
        this.clusterName = clusterName;
        this.host = host;
        this.port = port;
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
        this.query = query;
        this.fieldType = fieldType;
        this.thisShardId = thisShardId;
        this.canOptimizeJoins = canOptimizeJoins;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getLeftFieldname() {
        return leftFieldname;
    }

    public String getRightFieldname() {
        return rightFieldname;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public FastTermsResponse getFastTerms() {
        return fastTerms;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new ConstantScoreWeight(this) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                BitSet bitset = CrossJoinQueryExecutor.execute(
                        context,
                        type,
                        leftFieldname,
                        fieldType,
                        fastTerms
                );

                return bitset == null ? null : new ConstantScoreScorer(this, 0, new BitDocIdSet(bitset).iterator());
            }
        };
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        fastTerms = RESPONSE_CACHE.get(cacheKey);

        if (fastTerms == null) {
            synchronized (cacheKey) {
                fastTerms = FastTermsAction.INSTANCE.newRequestBuilder(getClient(clusterName, host, port))
                        .setIndices(index)
                        .setTypes(type)
                        .setFieldname(rightFieldname)
                        .setQuery(query)
                        .setSourceShard(canOptimizeJoins ? thisShardId : -1)
                        .get();

                if (fastTerms.getFailedShards() > 0)
                    throw new IOException(fastTerms.getShardFailures()[0].getCause());

                RESPONSE_CACHE.put(cacheKey, fastTerms);
            }
        }

        return CrossJoinQueryRewriteHelper.rewriteQuery(this);
    }

    private static TransportClient getClient(String clusterName, String host, int port) {
        return AccessController.doPrivileged((PrivilegedAction<TransportClient>) () -> {
            final String key = clusterName + host + port;
            synchronized (key.intern()) {
                TransportClient tc = CLIENTS.get(key);
                if (tc == null) {
                    int retries = 5;
                    while (true) {
                        try {
                            tc = new PreBuiltTransportClient(
                                    Settings.builder()
                                            .put("cluster.name", clusterName)
                                            .put("client.transport.ignore_cluster_name", true)
                                            .put("client.transport.sniff", true)
                                            .build(),
                                    Collections.singletonList(ZomboDBPlugin.class)
                            ).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                            break;
                        } catch (UnknownHostException uhe) {
                            throw new RuntimeException(uhe);
                        } catch (Throwable t) {
                            if (--retries > 0)
                                continue;
                            throw t;
                        }
                    }

                    CLIENTS.put(key, tc);

                }
                return tc;
            }
        });
    }

    @Override
    public String toString(String field) {
        return "cross_join(cluster=" + clusterName + ", host=" + host + ", port=" + port + ", index=" + index + ", type=" + type + ", left=" + leftFieldname + ", right=" + rightFieldname + ", query=" + query + ", shard=" + thisShardId + ", canOptimizeJoins=" + canOptimizeJoins + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;
        CrossJoinQuery other = (CrossJoinQuery) obj;
        return Objects.equals(cacheKey, other.cacheKey) &&
                Objects.equals(clusterName, other.clusterName) &&
                Objects.equals(host, other.host) &&
                Objects.equals(port, other.port) &&
                Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(leftFieldname, other.leftFieldname) &&
                Objects.equals(rightFieldname, other.rightFieldname) &&
                Objects.equals(query, other.query) &&
                Objects.equals(thisShardId, other.thisShardId) &&
                Objects.equals(canOptimizeJoins, other.canOptimizeJoins);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cacheKey, clusterName, host, port, index, type, leftFieldname, rightFieldname, query, thisShardId, canOptimizeJoins);
    }
}
