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
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.util.*;
import org.elasticsearch.common.hppc.IntObjectMap;
import org.elasticsearch.common.hppc.IntObjectOpenHashMap;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

final class VisibilityQueryHelper {

    private static final ConcurrentSkipListSet<Long> KNOWN_COMMITTED_XIDS = new ConcurrentSkipListSet<>();

    static IntObjectMap<FixedBitSet> determineVisibility(final Query query, final String field, final long myXid, final long xmin, final long xmax, final Set<Long> activeXids, IndexReader reader) throws IOException {
        final IntObjectMap<FixedBitSet> visibilityBitSets = new IntObjectOpenHashMap<>();
        final Set<BytesRef> updatedCtids = new HashSet<>();
        IndexSearcher searcher = new IndexSearcher(reader);

        //
        // search the "state" type and collect a distinct set of all the _ctids
        // these represent the records in the index that have been updated
        // used below to determine visibility
        //

        Terms ctidTerms = MultiFields.getTerms(reader, "_ctid");
        if (ctidTerms != null) {
            TermsEnum ctids = ctidTerms.iterator(null);
            BytesRef ctid;
            while ((ctid = ctids.next()) != null) {
                updatedCtids.add(BytesRef.deepCopyOf(ctid));
            }
        }

        if (updatedCtids.size() == 0)
            return visibilityBitSets;

        //
        // build a map of {@link VisibilityInfo} objects by each _prev_ctid
        //

        final Map<BytesRef, List<VisibilityInfo>> map = new HashMap<>();
        searcher.search(
                new FilteredQuery(Queries.newMatchAllQuery(), SearchContext.current().filterCache().cache(new TermsFilter(field, updatedCtids.toArray(new BytesRef[updatedCtids.size()])))),
                new ZomboDBTermsCollector(field) {
                    private SortedDocValues prevCtids;
                    private SortedNumericDocValues xids;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        xids.setDocument(doc);
                        long xid = xids.valueAt(0);
                        BytesRef prevCtid = prevCtids.get(doc);

                        List<VisibilityInfo> matchingDocs = map.get(prevCtid);

                        if (matchingDocs == null)
                            map.put(BytesRef.deepCopyOf(prevCtid), matchingDocs = new ArrayList<>());
                        matchingDocs.add(new VisibilityInfo(ord, maxdoc, doc, xid));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        prevCtids = FieldCache.DEFAULT.getTermsIndex(context.reader(), field);
                        xids = context.reader().getSortedNumericDocValues("_xid");
                        ord = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }
                }
        );

        if (map.isEmpty())
            return visibilityBitSets;

        //
        // pick out the first VisibilityInfo for each document that is visible & committed
        // and build a FixedBitSet for each reader 'ord' that contains visible
        // documents.  A map of these (key'd on reader ord) is what we return.
        //

        BytesRefBuilder bytesRefBuilder = new BytesRefBuilder() {
            /* overloaded to avoid making a copy of the byte array */
            @Override
            public BytesRef toBytesRef() {
                return new BytesRef(this.bytes(), 0, this.length());
            }
        };
        TermsEnum termsEnum = MultiFields.getFields(reader).terms("_zdb_committed_xid").iterator(null);
        for (List<VisibilityInfo> visibility : map.values()) {
            CollectionUtil.introSort(visibility, new Comparator<VisibilityInfo>() {
                @Override
                public int compare(VisibilityInfo o1, VisibilityInfo o2) {
                    return Long.compare(o2.xid, o1.xid);
                }
            });

            boolean foundVisible = false;
            for (VisibilityInfo mapping : visibility) {

                if (foundVisible || mapping.xid >= xmax || activeXids.contains(mapping.xid) || !isCommitted(termsEnum, mapping.xid, bytesRefBuilder)) {
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

    private static boolean isCommitted(TermsEnum termsEnum, long xid, BytesRefBuilder builder) throws IOException {
        if (KNOWN_COMMITTED_XIDS.contains(xid))
            return true;

        NumericUtils.longToPrefixCoded(xid, 0, builder);
        boolean isCommitted = termsEnum.seekExact(builder.toBytesRef());

        if (isCommitted)
            KNOWN_COMMITTED_XIDS.add(xid);

        builder.clear();
        return isCommitted;
    }
}
