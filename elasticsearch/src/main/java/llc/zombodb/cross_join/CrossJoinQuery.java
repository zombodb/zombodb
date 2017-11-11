package llc.zombodb.cross_join;

import llc.zombodb.ZomboDBPlugin;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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

public class CrossJoinQuery extends Query {

    private static final Map<String, TransportClient> CLIENTS = new ConcurrentHashMap<>();

    private final String clusterName;
    private final String host;
    private final int port;
    private final String index;
    private final String type;
    private final String leftFieldname;
    private final String rightFieldname;
    private final QueryBuilder query;

    public CrossJoinQuery(String clusterName, String host, int port, String index, String type, String leftFieldname, String rightFieldname, QueryBuilder query) {
        this.clusterName = clusterName;
        this.host = host;
        this.port = port;
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
        this.query = query;
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

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        TransportClient client = getClient(clusterName, host, port);

        return CrossJoinQueryRewriteHelper.rewriteQuery(client, CrossJoinQuery.this);
    }

    private static TransportClient getClient(String clusterName, String host, int port) {
        return AccessController.doPrivileged((PrivilegedAction<TransportClient>)() -> {
            String key = clusterName + host + port;
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
                            continue;;
                        throw t;
                    }
                }

                CLIENTS.put(key, tc);
            }

            return tc;
        });
    }

    @Override
    public String toString(String field) {
        return "cross_join(cluster=" + clusterName + ", host=" + host + ", port=" + port + ", index=" + index + ", type=" + type + ", left=" + leftFieldname + ", right=" + rightFieldname + ", query=" + query + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;
        CrossJoinQuery other = (CrossJoinQuery) obj;
        return Objects.equals(clusterName, other.clusterName) &&
                Objects.equals(host, other.host) &&
                Objects.equals(port, other.port) &&
                Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(leftFieldname, other.leftFieldname) &&
                Objects.equals(rightFieldname, other.rightFieldname) &&
                Objects.equals(query, other.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, host, port, index, type, leftFieldname, rightFieldname, query);
    }
}
