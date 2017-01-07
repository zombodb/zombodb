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

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TermlistRequest extends BroadcastOperationRequest<TermlistRequest> {

    private String fieldname;

    private String prefix;

    private Integer size = -1;
    private String startAt;

    TermlistRequest() {
    }

    public TermlistRequest(String... indices) {
        super(indices);
    }

    public void setFieldname(String field) {
        this.fieldname = field;
    }

    public String getFieldname() {
        return fieldname;
    }

    public void setPrefix(String term) {
        this.prefix = term;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setStartAt(String startAt) {
        this.startAt = startAt == null || startAt.length() == 0 ? null : startAt;
    }

    public String getStartAt() {
        return startAt;
    }

    public boolean hasUsableTermPrefix() {
        return prefix != null && prefix.length() > 0;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getSize() {
        return size;
    }

    static TermlistRequest from(StreamInput in) throws IOException {
        TermlistRequest request = new TermlistRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fieldname = in.readOptionalString();
        prefix = in.readOptionalString();
        size = in.readInt();
        startAt = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(fieldname);
        out.writeOptionalString(prefix);
        out.writeInt(size);
        out.writeOptionalString(startAt);
    }
}
