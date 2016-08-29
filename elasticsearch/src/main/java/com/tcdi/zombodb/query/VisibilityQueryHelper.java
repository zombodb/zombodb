/*
 * Copyright 2016 ZomboDB, LLC
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
package com.tcdi.zombodb.query;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.*;
import org.elasticsearch.common.hppc.*;
import org.elasticsearch.common.hppc.cursors.LongCursor;
import org.elasticsearch.common.hppc.cursors.LongObjectCursor;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

final class VisibilityQueryHelper {

    static IntObjectMap<FixedBitSet> determineVisibility(final String field, Query query, final long xmin, final long xmax, final Set<Long> activeXids, IndexReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);

        //
        // collect all the pkeys in the query that are likely visible to our current Postgres transaction
        //

        final LongObjectMap<List<VisibilityInfo>> map = new LongObjectOpenHashMap<>();
        searcher.search(
                query,
                new ZomboDBTermsCollector(field) {
                    private SortedNumericDocValues pkeys;
                    private SortedNumericDocValues xids;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        xids.setDocument(doc);
                        long xid = xids.valueAt(0);

                        if (xid >= xmax || activeXids.contains(xid))
                            return; // document definitely not visible to us

                        pkeys.setDocument(doc);
                        long pkey = pkeys.valueAt(0);

                        List<VisibilityInfo> matchingDocs = map.get(pkey);
                        if (matchingDocs == null) {
                            map.put(pkey, matchingDocs = new ArrayList<>());
                        }
                        matchingDocs.add(new VisibilityInfo(ord, maxdoc, doc, pkey, xid));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        pkeys = context.reader().getSortedNumericDocValues(field);
                        xids = context.reader().getSortedNumericDocValues("_xid");
                        ord = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }
                }
        );


        //
        // pick out the first VisibilityInfo for each document that is committed
        // and build a FixedBitSet for each reader 'ord' that contains visible
        // documents.  A map of these (key'd on reader ord) is what we return.
        //

        final IntObjectMap<FixedBitSet> visibilityBitSets = new IntObjectOpenHashMap<>();
        for (LongObjectCursor<List<VisibilityInfo>> cursor : map) {
            CollectionUtil.timSort(cursor.value, new Comparator<VisibilityInfo>() {
                @Override
                public int compare(VisibilityInfo o1, VisibilityInfo o2) {
                    return Long.compare(o2.xid, o1.xid);
                }
            });

            for (VisibilityInfo mapping : cursor.value) {
                // TODO: if (xid.didCommit) {
                    FixedBitSet visibilityBitset = visibilityBitSets.get(mapping.readerOrd);
                    if (visibilityBitset == null)
                        visibilityBitSets.put(mapping.readerOrd, visibilityBitset = new FixedBitSet(mapping.maxdoc));
                    visibilityBitset.set(mapping.docid);
                    break;
                // }
            }
        }

        if (visibilityBitSets.size() == 0)
            return null;

        return visibilityBitSets;
    }
}
