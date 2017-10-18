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
package com.tcdi.zombodb.query;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;

import java.io.IOException;
import java.util.*;

final class VisibilityQueryHelper {

    private static class HeapTuple implements Comparable<HeapTuple> {
        private int blockno, offno;
        private long xmin;
        private int cmin;
        private long xmax;
        private int cmax;
        private boolean hasMax;

        private int readerOrd = -1;
        private int maxdoc = -1;
        private int docid = -1;

        private final int hash;


        private HeapTuple(BytesRef bytes, boolean isxmin, ByteArrayDataInput in) {
            // lucene prefixes binary terms with a header of two variable length ints.
            // because we know how our binary data is constructed we can blindly
            // assume that the header length is 2 bytes.  1 byte for the number of items
            // and 1 byte for the first/only item's byte length, neither of which we need
            in.reset(bytes.bytes, 2, bytes.length-2);

            blockno = in.readVInt();
            offno = in.readVInt();
            if (in.getPosition() < bytes.length) {
                // more bytes, so we also have xmax and cmax to read
                if (isxmin) {
                    xmin = in.readVLong();
                    cmin = in.readVInt();
                } else {
                    xmax = in.readVLong();
                    cmax = in.readVInt();
                    hasMax = true;
                }
            }

            hash = blockno + (31 * offno);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            assert(obj instanceof HeapTuple);
            HeapTuple other = (HeapTuple) obj;
            return this.blockno == other.blockno && this.offno == other.offno;
        }

        @Override
        public int compareTo(HeapTuple other) {
            return this.blockno < other.offno ? -1 : this.blockno > offno ? 1 : this.offno - other.offno;
        }
    }

    private static void collectAbortedXids(IndexSearcher searcher, final Set<Long> abortedXids, final List<BytesRef> abortedXidsAsBytes) throws IOException {
        searcher.search(new XConstantScoreQuery(new TermFilter(new Term("_type", "aborted"))),
                new ZomboDBTermsCollector() {
                    SortedNumericDocValues _zdb_xid;

                    @Override
                    public void collect(int doc) throws IOException {
                        _zdb_xid.setDocument(doc);

                        long xid = _zdb_xid.valueAt(0);
                        BytesRefBuilder builder = new BytesRefBuilder();
                        NumericUtils.longToPrefixCoded(xid, 0, builder);

                        abortedXids.add(xid);
                        abortedXidsAsBytes.add(builder.get());
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        _zdb_xid = context.reader().getSortedNumericDocValues("_zdb_xid");
                    }
                }
        );
    }

    private static void collectMaxes(IndexSearcher searcher, final Map<HeapTuple, HeapTuple> tuples, final IntSet dirtyBlocks) throws IOException {
        abstract class Collector extends ZomboDBTermsCollector {
            ByteArrayDataInput in = new ByteArrayDataInput();
            BinaryDocValues _zdb_encoded_tuple;

            @Override
            public void setNextReader(AtomicReaderContext context) throws IOException {
                _zdb_encoded_tuple = context.reader().getBinaryDocValues("_zdb_encoded_tuple");
            }
        }

        if (dirtyBlocks != null) {
            searcher.search(new XConstantScoreQuery(new TermFilter(new Term("_type", "xmax"))),
                    new Collector() {
                        @Override
                        public void collect(int doc) throws IOException {
                            HeapTuple ctid = new HeapTuple(_zdb_encoded_tuple.get(doc), false, in);
                            tuples.put(ctid, ctid);
                            dirtyBlocks.add(ctid.blockno);
                        }
                    }
            );
        } else {
            searcher.search(new XConstantScoreQuery(new TermFilter(new Term("_type", "xmax"))),
                    new Collector() {
                        @Override
                        public void collect(int doc) throws IOException {
                            HeapTuple ctid = new HeapTuple(_zdb_encoded_tuple.get(doc), false, in);
                            tuples.put(ctid, ctid);
                        }
                    }
            );
        }
    }

    static Map<Integer, FixedBitSet> determineVisibility(final long myXid, final long myXmin, final long myXmax, final int myCommand, final Set<Long> activeXids, IndexSearcher searcher) throws IOException {
        int doccnt = 0;
        int xmax_cnt = 0;
        for (AtomicReaderContext context : searcher.getIndexReader().leaves()) {
            Terms terms;

            terms = context.reader().terms("_xmin");
            if (terms != null)
                doccnt += terms.getDocCount();

            terms = context.reader().terms("_xmax");
            if (terms != null)
                xmax_cnt += terms.getDocCount();
        }

        final boolean just_get_everything = xmax_cnt >= doccnt/3;

        final IntSet dirtyBlocks = just_get_everything ? null : new IntScatterSet();
        final List<HeapTuple> tuples = new ArrayList<>();
        final Map<HeapTuple, HeapTuple> modifiedTuples = new HashMap<>(xmax_cnt);

        collectMaxes(searcher, modifiedTuples, dirtyBlocks);

        final Set<Long> abortedXids = new HashSet<>();
        final List<BytesRef> abortedXidsAsBytes = new ArrayList<>();

        collectAbortedXids(searcher, abortedXids, abortedXidsAsBytes);

        final List<Filter> filters = new ArrayList<>();
        if (just_get_everything) {
            // if the number of docs with xmax values is at least 1/3 of the total docs
            // just go ahead and ask for everything.  This is much faster than asking
            // lucene to parse and lookup tens of thousands (or millions!) of individual
            // _uid values
            filters.add(new MatchAllDocsFilter());
        } else {
            // just look at all the docs on the blocks we've identified as dirty
            if (!dirtyBlocks.isEmpty()) {
                BytesRefBuilder builder = new BytesRefBuilder();
                List<BytesRef> tmp = new ArrayList<>();
                for (IntCursor blockNumber : dirtyBlocks) {
                    NumericUtils.intToPrefixCoded(blockNumber.value, 0, builder);
                    tmp.add(builder.toBytesRef());
                }
                filters.add(new TermsFilter("_zdb_blockno", tmp));
            }

            // we also need to examine docs that might be aborted or inflight on non-dirty pages

            final List<BytesRef> activeXidsAsBytes = new ArrayList<>(activeXids.size());
            for (Long xid : activeXids) {
                BytesRefBuilder builder = new BytesRefBuilder();
                NumericUtils.longToPrefixCoded(xid, 0, builder);
                activeXidsAsBytes.add(builder.toBytesRef());
            }

            if (!activeXids.isEmpty())
                filters.add(new TermsFilter("_xmin", activeXidsAsBytes));
            if (!abortedXids.isEmpty())
                filters.add(new TermsFilter("_xmin", abortedXidsAsBytes));
            filters.add(NumericRangeFilter.newLongRange("_xmin", myXmin, null, true, true));
        }

        searcher.search(new XConstantScoreQuery(
                        new AndFilter(
                                Arrays.asList(
                                        new TermFilter(new Term("_type", "data")),
                                        new OrFilter(filters)
                                )
                        )
                ),
                new ZomboDBTermsCollector() {
                    private final ByteArrayDataInput in = new ByteArrayDataInput();
                    private BinaryDocValues _zdb_encoded_tuple;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        HeapTuple ctid = new HeapTuple(_zdb_encoded_tuple.get(doc), true, in);

                        HeapTuple existingCtid = modifiedTuples.get(ctid);
                        if (existingCtid == null) {
                            boolean known_invisible = ctid.xmin >= myXmin || abortedXids.contains(ctid.xmin) || activeXids.contains(ctid.xmin);
                            if (!known_invisible)
                                return;     // we don't need to examine this one -- it's known visible to us

                            // wasn't part of our "xmax" lookups, so the doc hasn't been updated/deleted yet
                            //
                            // add to external list of tuples that don't have xmax data
                            // this avoids a HashMap.put() call, which in this tight loop is a good thing
                            tuples.add(existingCtid = ctid);
                        }

                        existingCtid.xmin = ctid.xmin;
                        existingCtid.cmin = ctid.cmin;

                        existingCtid.readerOrd = ord;
                        existingCtid.maxdoc = maxdoc;
                        existingCtid.docid = doc;
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        _zdb_encoded_tuple = context.reader().getBinaryDocValues("_zdb_encoded_tuple");
                        ord = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }
                }
        );

        final Map<Integer, FixedBitSet> visibilityBitSets = new HashMap<>();
        for (Iterable<HeapTuple> iterable : new Iterable[] { modifiedTuples.keySet(), tuples}) {
            for (HeapTuple ctid : iterable) {
                if (ctid.docid == -1) {
                    // we initially found this document in "xmax", but we didn't
                    // find it in "data" just up above.
                    //
                    // It must have been deleted by a vacuum between when we found it
                    // and then searched for it again
                    continue;
                }
                long xmin = ctid.xmin;
                int cmin = ctid.cmin;
                boolean xmax_is_null = !ctid.hasMax;
                long xmax = -1;
                long cmax = -1;

                if (!xmax_is_null) {
                    xmax = ctid.xmax;
                    cmax = ctid.cmax;
                }

                boolean xmin_is_committed = xmin < myXmax && !(xmin >= myXmax) && !activeXids.contains(xmin) && !abortedXids.contains(xmin);
                boolean xmax_is_committed = !xmax_is_null && xmax < myXmax && !(xmax >= myXmax) && !activeXids.contains(xmax) && !abortedXids.contains(xmax);

                if (
                        !(
                                (xmin == myXid && cmin < myCommand && (xmax_is_null || (xmax == myXid && cmax >= myCommand)))
                                        ||
                                (xmin_is_committed && (xmax_is_null || (xmax == myXid && cmax >= myCommand) || (xmax != myXid && !xmax_is_committed)))
                        )
                        ) {
                    // it's not visible to us
                    FixedBitSet visibilityBitset = visibilityBitSets.get(ctid.readerOrd);
                    if (visibilityBitset == null)
                        visibilityBitSets.put(ctid.readerOrd, visibilityBitset = new FixedBitSet(ctid.maxdoc));
                    visibilityBitset.set(ctid.docid);
                }
            }
        }

        return visibilityBitSets;
    }
}
