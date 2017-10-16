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

import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
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
import java.util.concurrent.ConcurrentSkipListSet;

final class VisibilityQueryHelper {

    private static void collectMaxes(IndexSearcher searcher, final Map<BytesRef, Long> xmax, final Map<BytesRef, Integer> cmax) throws IOException {
        searcher.search(new XConstantScoreQuery(new TermFilter(new Term("_type", "xmax"))),
                new ZomboDBTermsCollector() {
                    BinaryDocValues _uid;
                    SortedNumericDocValues _xmax;
                    SortedNumericDocValues _cmax;

                    @Override
                    public void collect(int doc) throws IOException {
                        _xmax.setDocument(doc);
                        _cmax.setDocument(doc);

                        String id = _uid.get(doc).utf8ToString();
                        String data_uid = "data#" + id.split("[#]")[1];

                        BytesRef uid = new BytesRef(data_uid);
                        xmax.put(uid, _xmax.valueAt(0));
                        cmax.put(uid, (int) _cmax.valueAt(0));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        _uid = FieldCache.DEFAULT.getTerms(context.reader(), "_uid", false);
                        _xmax = context.reader().getSortedNumericDocValues("_xmax");
                        _cmax = context.reader().getSortedNumericDocValues("_cmax");
                    }
                }
        );
    }

    private static void collectHints(IndexSearcher searcher, final List<BytesRef> hints) throws IOException {
        searcher.search(new XConstantScoreQuery(new TermFilter(new Term("_type", "hints"))),
                new ZomboDBTermsCollector() {
                    BinaryDocValues _uid;

                    @Override
                    public void collect(int doc) throws IOException {
                        String id = _uid.get(doc).utf8ToString();
                        String data_uid = "data#" + id.split("[#]")[1];

                        hints.add(new BytesRef(data_uid));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        _uid = FieldCache.DEFAULT.getTerms(context.reader(), "_uid", false);
                    }
                }
        );
    }


    static Map<Integer, FixedBitSet> determineVisibility(final long myXid, final long myXmin, final long myXmax, final int myCommand, final Set<Long> activeXids, IndexSearcher searcher) throws IOException {
        final Map<Integer, FixedBitSet> visibilityBitSets = new HashMap<>();
        final Map<BytesRef, Long> newXmaxes = new HashMap<>();
        final Map<BytesRef, Integer> newCmaxes = new HashMap<>();
        final List<BytesRef> hintCtids = new ArrayList<>();
        collectMaxes(searcher, newXmaxes, newCmaxes);
        collectHints(searcher, hintCtids);


        final List<BytesRef> activeXidsAsBytes = new ArrayList<>(activeXids.size());
        final List<BytesRef> ctids = new ArrayList<>(newXmaxes.keySet());
        final List<VisibilityInfo> visibilityList = new ArrayList<>(ctids.size());
        final List<Filter> filters = new ArrayList<>();

        for (Long xid : activeXids) {
            BytesRefBuilder builder = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(xid, 0, builder);
            activeXidsAsBytes.add(builder.toBytesRef());
        }

        if (!ctids.isEmpty())
            filters.add(new TermsFilter("_uid", ctids));
        if (!activeXids.isEmpty())
            filters.add(new TermsFilter("_xmin", activeXidsAsBytes));
        if (!hintCtids.isEmpty())
            filters.add(new TermsFilter("_uid", hintCtids));

        searcher.search(new XConstantScoreQuery(
                        new AndFilter(
                                Arrays.asList(
                                        new TermFilter(new Term("_type", "data")),
                                        new OrFilter(filters)
                                )
                        )
                ),
                new ZomboDBTermsCollector() {
                    private BinaryDocValues _uid;
                    private SortedNumericDocValues _xmin;
                    private SortedNumericDocValues _cmin;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        _xmin.setDocument(doc);
                        _cmin.setDocument(doc);

                        long xmin = _xmin.valueAt(0);
                        int cmin = (int) _cmin.valueAt(0);
                        BytesRef id = BytesRef.deepCopyOf(_uid.get(doc));

                        visibilityList.add(new VisibilityInfo(ord, maxdoc, doc, id, xmin, cmin));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        _uid = FieldCache.DEFAULT.getTerms(context.reader(), "_uid", false);
                        _xmin = context.reader().getSortedNumericDocValues("_xmin");
                        _cmin = context.reader().getSortedNumericDocValues("_cmin");
                        ord = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }
                }
        );

        if (visibilityList.size() > 0) {
            Terms committedXidsTerms = MultiFields.getFields(searcher.getIndexReader()).terms("_zdb_xid");
            TermsEnum committedXidsEnum = committedXidsTerms == null ? null : committedXidsTerms.iterator(null);

            InvisibilityMarker im = new InvisibilityMarker(visibilityBitSets);
            for (VisibilityInfo vi : visibilityList) {
                long xmin = vi.xmin;
                int cmin = vi.cmin;
                Long xmax = newXmaxes.get(vi.id);
                Integer cmax = newCmaxes.get(vi.id);

                boolean xmin_is_committed = xmin < myXmax && !(xmin >= myXmax) && !activeXids.contains(xmin) && isCommitted(committedXidsEnum, xmin);
                boolean xmax_is_committed = xmax != null && xmax < myXmax && !(xmax >= myXmax) && !activeXids.contains(xmax) && isCommitted(committedXidsEnum, xmax);

                if (
                        !(
                                (xmin == myXid && cmin < myCommand && (xmax == null || (xmax == myXid && cmax >= myCommand)))
                                        ||
                                (xmin_is_committed && (xmax == null || (xmax == myXid && cmax >= myCommand) || (xmax != myXid && !xmax_is_committed)))
                        )
                        ) {
                    // it's not visible to us
                    im.setVisibilityInfo(vi);
                    im.invisible();
                    continue;
                }

                if (activeXids.contains(xmin))
                    System.err.println ("xmin active: " + xmin + ", xmax=" + xmax + "; myXid=" + myXid + ", myXmin=" + myXmin + ", myXmax=" + myXmax + ", min_is_committed=" + xmin_is_committed + ", max_is_committed=" + xmax_is_committed);
            }
        }

        return visibilityBitSets;
    }

    static class InvisibilityMarker {
        Map<Integer, FixedBitSet> visibilityBitSets;
        VisibilityInfo vi;

        InvisibilityMarker(Map<Integer, FixedBitSet> visibilityBitSets) {
            this.visibilityBitSets = visibilityBitSets;
            this.vi = vi;
        }

        void invisible() {
            FixedBitSet visibilityBitset = visibilityBitSets.get(vi.readerOrd);
            if (visibilityBitset == null)
                visibilityBitSets.put(vi.readerOrd, visibilityBitset = new FixedBitSet(vi.maxdoc));
            visibilityBitset.set(vi.docid);
        }

        void setVisibilityInfo(VisibilityInfo visibilityInfo) {
            this.vi = visibilityInfo;
        }
    }

    private static final ConcurrentSkipListSet<Long> KNOWN_COMMITTED_XIDS = new ConcurrentSkipListSet<>();

    private static boolean isCommitted(TermsEnum termsEnum, Long xid) throws IOException {
        if (xid == null)
            return false;

        if (termsEnum == null)
            return false;

        if (KNOWN_COMMITTED_XIDS.contains(xid))
            return true;

        BytesRefBuilder builder = new BytesRefBuilder();
        NumericUtils.longToPrefixCoded(xid, 0, builder);
        boolean isCommitted = termsEnum.seekExact(builder.get());

        if (isCommitted)
            KNOWN_COMMITTED_XIDS.add(xid);

        return isCommitted;
    }

}
