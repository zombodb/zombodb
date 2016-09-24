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
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.hppc.IntObjectMap;
import org.elasticsearch.common.hppc.IntObjectOpenHashMap;
import org.elasticsearch.common.lucene.search.Queries;

import java.io.IOException;
import java.util.*;

final class VisibilityQueryHelper {

    static IntObjectMap<FixedBitSet> determineVisibility(final String field, final long myXid, final long xmin, final long xmax, final Set<Long> activeXids, IndexReader reader) throws IOException {
        final IntObjectMap<FixedBitSet> visibilityBitSets = new IntObjectOpenHashMap<>();
        final Set<Long> committedXids = new HashSet<>();
        IndexSearcher searcher = new IndexSearcher(reader);

        //
        // collect all the committed transaction ids from the index (_zdb_committed_xid)
        //

        for (AtomicReaderContext context : reader.getContext().leaves()) {
            FieldCache.Longs longs = FieldCache.DEFAULT.getLongs(context.reader(), "_zdb_committed_xid", false);
            Bits docsWithValues = FieldCache.DEFAULT.getDocsWithField(context.reader(), "_zdb_committed_xid");
            int maxdoc = docsWithValues.length();

            for (int i = 0; i < maxdoc; i++) {
                if (docsWithValues.get(i))
                    committedXids.add(longs.get(i));
            }
        }


        //
        // similar to the above but with the _prev_ctid field
        // scan all documents in the shard to
        // find all the _prev_ctid values with a count >1
        // then form a query that ors that list together
        //

        Set<BytesRef> multiples = new HashSet<>();
        Map<BytesRef, BytesRef> singles = new HashMap<>();
        TermsEnum termsEnum = null;
        for (AtomicReaderContext context : reader.leaves()) {
            Bits liveDocs = context.reader().getLiveDocs();
            Terms terms = context.reader().terms("_prev_ctid");
            if (terms == null)
                continue;

            termsEnum = terms.iterator(termsEnum);

            SortedDocValues ssdv = FieldCache.DEFAULT.getTermsIndex(context.reader(), "_prev_ctid");
            for (int i=0; i<context.reader().maxDoc(); i++) {
                if (liveDocs == null || liveDocs.get(i)) {
                    BytesRef prevCtid = ssdv.get(i);
                    if (prevCtid.length == 0)
                        continue;

                    BytesRef single = singles.get(prevCtid);

                    if (single != null) {
                        multiples.add(single);
                    } else {
                        BytesRef copy = BytesRef.deepCopyOf(prevCtid);
                        singles.put(copy, copy);
                    }
                }

            }

//            DocsEnum docsEnum = null;
//            BytesRef prevCtid;
//            while ((prevCtid = termsEnum.next()) != null) {
//                if ( (docsEnum = termsEnum.docs(liveDocs, docsEnum)).nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
//                    BytesRef single = singles.get(prevCtid);
//
//                    if (single != null) {
//                        multiples.add(single);
//                    } else {
//                        BytesRef copy = BytesRef.deepCopyOf(prevCtid);
//                        singles.put(copy, copy);
//                    }
//                }
//            }
        }

        if (multiples.size() == 0)
            return visibilityBitSets;

        TermsFilter filter = new TermsFilter("_prev_ctid", multiples.toArray(new BytesRef[multiples.size()]));
        Query query = new FilteredQuery(Queries.newMatchAllQuery(), filter);

        //
        // build a map of {@link VisibilityInfo} objects by each _prev_ctid
        //

        final Map<String, List<VisibilityInfo>> map = new HashMap<>();
        searcher.search(
                query,
                new ZomboDBTermsCollector(field) {
                    private SortedDocValues prevCtids;
                    private SortedNumericDocValues xids;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        xids.setDocument(doc);
                        long xid = xids.valueAt(0);
                        String prevCtid = prevCtids.get(doc).utf8ToString();

                        List<VisibilityInfo> matchingDocs = map.get(prevCtid);

                        if (matchingDocs == null)
                            map.put(prevCtid, matchingDocs = new ArrayList<>());
                        matchingDocs.add(new VisibilityInfo(ord, maxdoc, doc, xid));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        prevCtids = FieldCache.DEFAULT.getTermsIndex(context.reader(), "_prev_ctid");
                        xids = context.reader().getSortedNumericDocValues("_xid");
                        ord = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }
                }
        );

        //
        // pick out the first VisibilityInfo for each document that is visible & committed
        // and build a FixedBitSet for each reader 'ord' that contains visible
        // documents.  A map of these (key'd on reader ord) is what we return.
        //

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
