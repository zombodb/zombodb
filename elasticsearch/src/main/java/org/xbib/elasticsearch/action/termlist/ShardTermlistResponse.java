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

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ShardTermlistResponse extends BroadcastShardOperationResponse {

    private String index;

    private int numDocs;

    private List<TermInfo> termsList;

    ShardTermlistResponse() {
    }

    public ShardTermlistResponse(String index, ShardId shardId, int numDocs, List<TermInfo> termsList) {
        super(shardId);
        this.numDocs = numDocs;
        this.index = index;
        this.termsList = termsList;
    }

    public String getIndex() {
        return index;
    }

    public int getNumDocs() {
        return numDocs;
    }

    public List<TermInfo> getTermList() {
        return termsList;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numDocs = in.readInt();
        int n = in.readInt();
        termsList = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            TermInfo ti = new TermInfo();
            ti.readFrom(in);
            termsList.add(ti);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(numDocs);
        out.writeInt(termsList.size());
        for (TermInfo ti : termsList)
            ti.writeTo(out);
    }
}