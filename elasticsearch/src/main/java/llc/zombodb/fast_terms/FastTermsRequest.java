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

import java.io.IOException;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;

public class FastTermsRequest extends BroadcastRequest<FastTermsRequest> {
    private String[] types;
    private String fieldname;
    private int sourceShardId = -1;
    private QueryBuilder query;

    static FastTermsRequest from(StreamInput in) throws IOException {
        FastTermsRequest request = new FastTermsRequest();
        request.readFrom(in);
        return request;
    }

    public String[] types() {
        return types;
    }

    public void types(String[] types) {
        this.types = types;
    }

    public void query(QueryBuilder query) {
        this.query = query;
    }

    public QueryBuilder query() {
        return query;
    }

    public String fieldname() {
        return fieldname;
    }

    public void fieldname(String fieldname) {
        this.fieldname = fieldname;
    }

    public int sourceShardId() {
        return sourceShardId;
    }

    public void sourceShardId(int sourceShardId) {
        this.sourceShardId = sourceShardId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        types = in.readStringArray();
        fieldname = in.readString();
        sourceShardId = in.readInt();
        query = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(types);
        out.writeString(fieldname);
        out.writeInt(sourceShardId);
        out.writeNamedWriteable(query);
    }
}
