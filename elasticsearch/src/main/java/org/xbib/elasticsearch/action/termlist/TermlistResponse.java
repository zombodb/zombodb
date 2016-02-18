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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * A response for termlist action.
 */
public class TermlistResponse extends BroadcastOperationResponse {

    private int numdocs;

    private List<TermInfo> termsList;

    TermlistResponse() {
    }

    TermlistResponse(int totalShards, int successfulShards, int failedShards,
                     List<ShardOperationFailedException> shardFailures,
                     int numdocs,
                     List<TermInfo> termsList) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.numdocs = numdocs;
        this.termsList = termsList;
    }

    public int getNumDocs() {
        return numdocs;
    }

    public List<TermInfo> getTermlist() {
        return termsList;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numdocs = in.readInt();
        int n = in.readInt();
        termsList = new LinkedList<>();
        for (int i=0; i<n; i++) {
            TermInfo ti = new TermInfo();
            ti.readFrom(in);
            termsList.add(ti);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(numdocs);
        out.writeInt(termsList.size());
        for (TermInfo ti : termsList)
            ti.writeTo(out);
    }
}