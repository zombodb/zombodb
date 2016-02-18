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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;

public class TermInfo implements Streamable, ToXContent, Serializable {

    private String term;
    private Integer docFreq;
    private Long totalFreq;

    public TermInfo() {

    }

    public TermInfo(String term, Integer docFreq, Long totalFreq) {
        this.term = term;
        this.docFreq = docFreq;
        this.totalFreq = totalFreq;
    }

    public String getTerm() {
        return term;
    }

    public TermInfo setTerm(String term) {
        this.term = term;
        return this;
    }

    public TermInfo setDocFreq(int docFreq) {
        this.docFreq = docFreq;
        return this;
    }

    public Integer getDocFreq() {
        return docFreq;
    }

    public TermInfo setTotalFreq(long totalFreq) {
        this.totalFreq = totalFreq;
        return this;
    }

    public Long getTotalFreq() {
        return totalFreq;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boolean b;

        term = in.readString();

        b = in.readBoolean();
        if (b) {
            setDocFreq(in.readInt());
        }

        b = in.readBoolean();
        if (b) {
            setTotalFreq(in.readVLong());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(term);

        if (docFreq != null) {
            out.writeBoolean(true);
            out.writeInt(docFreq);
        } else {
            out.writeBoolean(false);
        }
        if (totalFreq != null) {
            out.writeBoolean(true);
            out.writeVLong(totalFreq);
        } else {
            out.writeBoolean(false);
        }
    }

    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (totalFreq != null) {
            builder.field("totalfreq", totalFreq);
        }
        if (docFreq != null) {
            builder.field("docfreq", docFreq);
        }
        return builder;
    }
}
