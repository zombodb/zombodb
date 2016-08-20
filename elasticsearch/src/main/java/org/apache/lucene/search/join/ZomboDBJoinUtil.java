package org.apache.lucene.search.join;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.hppc.*;

import java.io.IOException;

/**
 * Utility for query time joining using ZomboDBTermsQuery and TermsCollector.
 * <p>
 * Copied (and modified) from {@link org.apache.lucene.search.join.JoinUtil}
 *
 * @lucene.experimental
 */
public final class ZomboDBJoinUtil {

    // No instances allowed
    private ZomboDBJoinUtil() {
    }

    public static LongObjectMap<IntArrayList> createJoinQuery(String field, Query fromQuery, IndexReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        final LongSet values = new LongOpenHashSet();
        searcher.search(new ConstantScoreQuery(fromQuery), new TermsCollector(field) {
            private SortedNumericDocValues fromDocTerms;

            @Override
            public void collect(int doc) throws IOException {
                fromDocTerms.setDocument(doc);
                long value = fromDocTerms.valueAt(0);
                values.add(value);
            }

            @Override
            public void setNextReader(AtomicReaderContext context) throws IOException {
                fromDocTerms = context.reader().getSortedNumericDocValues(field);
            }
        });

        if (values.size() == 0)
            return null;

        final LongObjectMap<IntArrayList> map = new LongObjectOpenHashMap<>(values.size());
        searcher.search(new ConstantScoreQuery(new ZomboDBTermsQuery(field, fromQuery, values)),
                new TermsCollector(field) {
                    private SortedNumericDocValues fromDocTerms;

                    @Override
                    public void collect(int doc) throws IOException {
                        fromDocTerms.setDocument(doc);
                        long value = fromDocTerms.valueAt(0);
                        IntArrayList matchingDocs = map.get(value);
                        if (matchingDocs == null) {
                            map.put(value, matchingDocs = new IntArrayList(1));
                        }
                        matchingDocs.add(doc);
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        fromDocTerms = context.reader().getSortedNumericDocValues(field);
                    }
                }
        );
        return map;
    }
}
