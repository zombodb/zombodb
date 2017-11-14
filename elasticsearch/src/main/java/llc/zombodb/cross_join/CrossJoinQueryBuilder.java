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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class CrossJoinQueryBuilder extends AbstractQueryBuilder<CrossJoinQueryBuilder> {
    public static final String NAME = "cross_join";

    private static final AtomicInteger SEQ = new AtomicInteger(0);
    private String cacheKey;
    private String clusterName;
    private String host;
    private int port;
    private String index;
    private String type;
    private String leftFieldname;
    private String rightFieldname;
    private QueryBuilder query;

    public CrossJoinQueryBuilder() {
        super();
        cacheKey = System.currentTimeMillis() + ":" + SEQ.getAndIncrement();
    }

    public CrossJoinQueryBuilder(StreamInput in) throws IOException {
        super(in);
        cacheKey = in.readString();
        clusterName = in.readString();
        host = in.readString();
        port = in.readInt();
        index = in.readString();
        type = in.readString();
        leftFieldname = in.readString();
        rightFieldname = in.readString();
        query = in.readNamedWriteable(QueryBuilder.class);
    }

    public CrossJoinQueryBuilder(String cacheKey, String clusterName, String host, int port, String index, String type, String leftFieldname, String rightFieldname, QueryBuilder query) {
        this.cacheKey = cacheKey;
        this.clusterName = clusterName;
        this.host = host;
        this.port = port;
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
        this.query = query;
    }

    public CrossJoinQueryBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    public CrossJoinQueryBuilder clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public CrossJoinQueryBuilder host(String host) {
        this.host = host;
        return this;
    }

    public CrossJoinQueryBuilder port(int port) {
        this.port = port;
        return this;
    }

    public CrossJoinQueryBuilder index(String index) {
        this.index = index;
        return this;
    }

    public CrossJoinQueryBuilder type(String type) {
        this.type = type;
        return this;
    }

    public CrossJoinQueryBuilder leftFieldname(String leftFieldname) {
        this.leftFieldname = leftFieldname;
        return this;
    }

    public CrossJoinQueryBuilder rightFieldname(String rightFieldname) {
        this.rightFieldname = rightFieldname;
        return this;
    }

    public CrossJoinQueryBuilder query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(cacheKey);
        out.writeString(clusterName);
        out.writeString(host);
        out.writeInt(port);
        out.writeString(index);
        out.writeString(type);
        out.writeString(leftFieldname);
        out.writeString(rightFieldname);
        out.writeNamedWriteable(query);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("cache_key", cacheKey);
        builder.field("cluster_name", clusterName);
        builder.field("host", host);
        builder.field("port", port);
        builder.field("index", index);
        builder.field("type", type);
        builder.field("left_fieldname", leftFieldname);
        builder.field("right_fieldname", rightFieldname);
        builder.field("query", query);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new CrossJoinQuery(cacheKey, clusterName, host, port, index, type, leftFieldname, rightFieldname, query);
    }

    @Override
    protected boolean doEquals(CrossJoinQueryBuilder other) {
        return Objects.equals(cacheKey, other.cacheKey) &&
                Objects.equals(clusterName, other.clusterName) &&
                Objects.equals(host, other.host) &&
                Objects.equals(port, other.port) &&
                Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(leftFieldname, other.leftFieldname) &&
                Objects.equals(rightFieldname, other.rightFieldname) &&
                Objects.equals(query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(cacheKey, clusterName, host, port, index, type, leftFieldname, rightFieldname, query);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static final ObjectParser<CrossJoinQueryBuilder, QueryParseContext> PARSER = new ObjectParser<>(NAME, CrossJoinQueryBuilder::new);

    static {
        declareStandardFields(PARSER);
    }

    public static Optional<CrossJoinQueryBuilder> fromXContent(QueryParseContext context) {
        try {
            return Optional.of(PARSER.apply(context.parser(), context));
        } catch (IllegalArgumentException e) {
            throw new ParsingException(context.parser().getTokenLocation(), e.getMessage(), e);
        }
    }


}
