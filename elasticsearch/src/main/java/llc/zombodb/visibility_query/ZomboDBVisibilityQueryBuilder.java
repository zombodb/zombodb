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
package llc.zombodb.visibility_query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class ZomboDBVisibilityQueryBuilder extends AbstractQueryBuilder<ZomboDBVisibilityQueryBuilder> {
    public static final String NAME = "visibility";

    private long myXid;
    private long xmin;
    private int commandid;
    private long xmax;
    private long[] activeXids;

    public ZomboDBVisibilityQueryBuilder() {

    }

    public ZomboDBVisibilityQueryBuilder(StreamInput in) throws IOException {
        myXid = in.readLong();
        xmin = in.readLong();
        xmax = in.readLong();
        commandid = in.readInt();
        activeXids = in.readLongArray();
    }

    public ZomboDBVisibilityQueryBuilder(long myXid, long xmin, long xmax, int commandid, long[] activeXids) {
        this.myXid = myXid;
        this.xmin = xmin;
        this.xmax = xmax;
        this.commandid = commandid;
        this.activeXids = activeXids;
    }

    public ZomboDBVisibilityQueryBuilder myXid(long myXid) {
        this.myXid = myXid;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder xmin(long xmin) {
        this.xmin = xmin;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder xmax(long xmax) {
        this.xmax = xmax;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder commandId(int commandid) {
        this.commandid = commandid;
        return this;
    }

    public ZomboDBVisibilityQueryBuilder activeXids(long[] xids) {
        activeXids = xids;
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(myXid);
        out.writeLong(xmin);
        out.writeLong(xmax);
        out.writeInt(commandid);
        out.writeLongArray(activeXids);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new ZomboDBVisibilityQuery(myXid, xmin, xmax, commandid, activeXids);
    }

    @Override
    protected boolean doEquals(ZomboDBVisibilityQueryBuilder other) {
        return Objects.equals(myXid, other.myXid) &&
                Objects.equals(xmin, other.xmin) &&
                Objects.equals(xmax, other.xmax) &&
                Objects.equals(commandid, other.commandid) &&
                Arrays.equals(activeXids, other.activeXids);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(myXid, xmin, xmax, commandid, activeXids);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field("myxid", myXid);
        builder.field("xmin", xmin);
        builder.field("xmax", xmax);
        builder.field("commandid", commandid);
        builder.field("active_xids", activeXids);
        builder.endObject();
    }
}
