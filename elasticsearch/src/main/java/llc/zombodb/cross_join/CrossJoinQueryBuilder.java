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

import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.FastTermsResponse;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class CrossJoinQueryBuilder extends AbstractQueryBuilder<CrossJoinQueryBuilder> {
    public static final String NAME = "cross_join";

    private String index;
    private String type;
    private String leftFieldname;
    private String rightFieldname;
    private QueryBuilder query;
    private boolean canOptimizeJoins;
    private FastTermsResponse fastTerms;

    public CrossJoinQueryBuilder() {
        super();
    }

    public CrossJoinQueryBuilder(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        type = in.readString();
        leftFieldname = in.readString();
        rightFieldname = in.readString();
        query = in.readNamedWriteable(QueryBuilder.class);
        canOptimizeJoins = in.readBoolean();
        if (in.readBoolean())
            fastTerms = new FastTermsResponse(in);
    }

    public CrossJoinQueryBuilder(String index, String type, String leftFieldname, String rightFieldname, QueryBuilder query, boolean canOptimizeJoins, FastTermsResponse fastTerms) {
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
        this.query = query;
        this.canOptimizeJoins = canOptimizeJoins;
        this.fastTerms = fastTerms;
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

    public CrossJoinQueryBuilder fastTerms(FastTermsResponse fastTerms) {
        this.fastTerms = fastTerms;
        return this;
    }

    public boolean canOptimizeJoins() {
        return canOptimizeJoins;
    }

    public CrossJoinQueryBuilder canOptimizeJoins(boolean canOptimizeJoins) {
        this.canOptimizeJoins = canOptimizeJoins;
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeString(leftFieldname);
        out.writeString(rightFieldname);
        out.writeNamedWriteable(query);
        out.writeBoolean(canOptimizeJoins);
        out.writeBoolean(fastTerms != null);
        if (fastTerms != null)
            fastTerms.writeTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("index", index);
        builder.field("type", type);
        builder.field("left_fieldname", leftFieldname);
        builder.field("right_fieldname", rightFieldname);
        builder.field("can_optimize_joins", canOptimizeJoins);
        builder.field("query", query);
        builder.field("fast_terms", fastTerms);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        FastTermsResponse fastTerms = this.fastTerms;

        if (fastTerms == null)
            fastTerms = FastTermsAction.INSTANCE.newRequestBuilder(context.getClient())
                .setIndices(index)
                .setTypes(type)
                .setFieldname(rightFieldname)
                .setQuery(query)
                .setSourceShard(canOptimizeJoins ? context.getShardId() : -1)
                    .get(TimeValue.timeValueSeconds(300));

        return new CrossJoinQuery(index, type, leftFieldname, rightFieldname, query, canOptimizeJoins, context.fieldMapper(leftFieldname).typeName(), context.getShardId(), fastTerms);
    }

    @Override
    protected boolean doEquals(CrossJoinQueryBuilder other) {
        return Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(leftFieldname, other.leftFieldname) &&
                Objects.equals(rightFieldname, other.rightFieldname) &&
                Objects.equals(query, other.query) &&
                Objects.equals(canOptimizeJoins, canOptimizeJoins);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(index, type, leftFieldname, rightFieldname, query, canOptimizeJoins);
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
