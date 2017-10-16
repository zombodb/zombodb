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
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;

import java.io.IOException;
import java.util.*;

final class VisibilityQueryHelper {

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

    static Map<Integer, FixedBitSet> determineVisibility(final long myXid, final long myXmin, final long myXmax, final int myCommand, final Set<Long> activeXids, IndexSearcher searcher) throws IOException {
        final Map<Integer, FixedBitSet> visibilityBitSets = new HashMap<>();
        final Map<BytesRef, Long> newXmaxes = new HashMap<>();
        final Map<BytesRef, Integer> newCmaxes = new HashMap<>();
        final Set<Long> abortedXids = new HashSet<>();
        final List<BytesRef> abortedXidsAsBytes = new ArrayList<>();

        collectAbortedXids(searcher, abortedXids, abortedXidsAsBytes);
        collectMaxes(searcher, newXmaxes, newCmaxes);

        final List<BytesRef> activeXidsAsBytes = new ArrayList<>(activeXids.size());
        final List<BytesRef> ctidsWithXmax = new ArrayList<>(newXmaxes.keySet());
        final List<VisibilityInfo> visibilityList = new ArrayList<>(ctidsWithXmax.size());
        final List<Filter> filters = new ArrayList<>();

        for (Long xid : activeXids) {
            BytesRefBuilder builder = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(xid, 0, builder);
            activeXidsAsBytes.add(builder.toBytesRef());
        }

        if (!ctidsWithXmax.isEmpty())
            filters.add(new TermsFilter("_uid", ctidsWithXmax));
        if (!activeXids.isEmpty())
            filters.add(new TermsFilter("_xmin", activeXidsAsBytes));
        if (!abortedXids.isEmpty())
            filters.add(new TermsFilter("_xmin", abortedXidsAsBytes));
        filters.add(NumericRangeFilter.newLongRange("_xmin", myXmin, null, true, true));

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
           for (VisibilityInfo vi : visibilityList) {
                long xmin = vi.xmin;
                int cmin = vi.cmin;
                Long xmax = newXmaxes.get(vi.id);
                Integer cmax = newCmaxes.get(vi.id);

                boolean xmin_is_committed = xmin < myXmax && !(xmin >= myXmax) && !activeXids.contains(xmin) && !abortedXids.contains(xmin);
                boolean xmax_is_committed = xmax != null && xmax < myXmax && !(xmax >= myXmax) && !activeXids.contains(xmax) && !abortedXids.contains(xmax);

                if (
                        !(
                                (xmin == myXid && cmin < myCommand && (xmax == null || (xmax == myXid && cmax >= myCommand)))
                                        ||
                                (xmin_is_committed && (xmax == null || (xmax == myXid && cmax >= myCommand) || (xmax != myXid && !xmax_is_committed)))
                        )
                        ) {
                    // it's not visible to us
                    FixedBitSet visibilityBitset = visibilityBitSets.get(vi.readerOrd);
                    if (visibilityBitset == null)
                        visibilityBitSets.put(vi.readerOrd, visibilityBitset = new FixedBitSet(vi.maxdoc));
                    visibilityBitset.set(vi.docid);
                    continue;
                }
            }
        }

        return visibilityBitSets;
    }
}
