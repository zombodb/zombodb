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

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.util.*;
import org.elasticsearch.common.hppc.*;

import java.io.IOException;
import java.util.*;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

final class VisibilityQueryHelper {

    static IntObjectMap<FixedBitSet> determineVisibility(final String field, Query query, final long myXid, final long xmin, final long xmax, final Set<Long> activeXids, final Set<Long> committedXids, IndexReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);

        //
        // collect all the _routing values in the query that are likely visible to our current Postgres transaction
        //

        final Map<String, List<VisibilityInfo>> map = new HashMap<>();
        searcher.search(
                query.rewrite(reader),
                new ZomboDBTermsCollector(field) {
                    private SortedSetDocValues prevCtids;
                    private SortedNumericDocValues xids;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        if (xids == null)
                            return;

                        xids.setDocument(doc);
                        prevCtids.setDocument(doc);
                        long xid = xids.valueAt(0);

                        long nextOrd;
                        if ((nextOrd = prevCtids.nextOrd()) != NO_MORE_ORDS) {
                            String routing = prevCtids.lookupOrd(nextOrd).utf8ToString();
                            List<VisibilityInfo> matchingDocs = map.get(routing);

                            if (matchingDocs == null)
                                map.put(routing, matchingDocs = new ArrayList<>());
                            matchingDocs.add(new VisibilityInfo(ord, maxdoc, doc, xid));
                        }
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        prevCtids = context.reader().getSortedSetDocValues(field);
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
        for (List<VisibilityInfo> visibility : map.values()) {
            CollectionUtil.timSort(visibility, new Comparator<VisibilityInfo>() {
                @Override
                public int compare(VisibilityInfo o1, VisibilityInfo o2) {
                    return Long.compare(o2.xid, o1.xid);
                }
            });

            boolean foundVisible = false;
            for (VisibilityInfo mapping : visibility) {

                if (foundVisible || mapping.xid >= xmax || activeXids.contains(mapping.xid) || !committedXids.contains(mapping.xid)) {
                    // document is not visible to us
                    FixedBitSet visibilityBitset = visibilityBitSets.get(mapping.readerOrd);
                    if (visibilityBitset == null)
                        visibilityBitSets.put(mapping.readerOrd, visibilityBitset = new FixedBitSet(mapping.maxdoc));
                    visibilityBitset.set(mapping.docid);
                } else {
                    foundVisible = true;
                }
            }
        }

        return visibilityBitSets;
    }
}
