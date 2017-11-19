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

import llc.zombodb.cross_join.collectors.CrossJoinCollector;
import llc.zombodb.fast_terms.FastTermsResponse;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;

import java.io.IOException;

class CrossJoinQueryExecutor {

    static BitSet execute(LeafReaderContext context, String type, String fieldname, String fieldType, FastTermsResponse fastTerms) throws IOException {
        CrossJoinCollector collector;

        switch(fieldType) {
            case "integer":
                collector = CrossJoinCollector.create(fieldname, fastTerms.getNumberLookup());
                break;
            case "long":
                collector = CrossJoinCollector.create(fieldname, fastTerms.getNumberLookup());
                break;
            case "keyword":
                collector = CrossJoinCollector.create(fieldname, fastTerms.getStringArray());
                break;
            default:
                throw new RuntimeException("Unsupported field type [" + fieldType + "] for [" + fieldname + "]");
        }

        collector.doSetNextReader(context);
        for (int i=0; i<context.reader().maxDoc(); i++) {
            collector.collect(i);
        }

        return collector.getBitset();
    }
}
