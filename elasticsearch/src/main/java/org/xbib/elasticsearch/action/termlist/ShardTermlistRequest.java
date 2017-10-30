/**
 * Portions Copyright (C) 2011-2015 Jörg Prante
 * Portions Copyright (C) 2017 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.xbib.elasticsearch.action.termlist;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

class ShardTermlistRequest extends BroadcastShardRequest {

    private String index;

    private TermlistRequest request;

    ShardTermlistRequest() {
    }

    public ShardTermlistRequest(String index, ShardId shardId, TermlistRequest request) {
        super(shardId, request);
        this.index = index;
        this.request = request;
    }

    public String getIndex() {
        return index;
    }

    public TermlistRequest getRequest() {
        return request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        request = TermlistRequest.from(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        request.writeTo(out);
    }
}
