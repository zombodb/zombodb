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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ZomboDBTermsCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

final class VisibilityQueryHelper {

    private static List<BytesRef> findAbortedXids(IndexSearcher searcher, final long myXid, final Set<Long> lookup) throws IOException {
        final List<BytesRef> abortedXids = new ArrayList<>();

        searcher.search(new XConstantScoreQuery(SearchContext.current().filterCache().cache(new TermFilter(new Term("_type", "aborted")))),
                new ZomboDBTermsCollector("_ctid") {
                    SortedNumericDocValues xids;

                    @Override
                    public void collect(int doc) throws IOException {
                        if (xids == null)
                            return;
                        xids.setDocument(doc);
                        long xid = xids.valueAt(0);
                        BytesRefBuilder builder = new BytesRefBuilder();
                        NumericUtils.longToPrefixCoded(xid, 0, builder);
                        abortedXids.add(builder.toBytesRef());
                        lookup.add(xid);
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        xids = context.reader().getSortedNumericDocValues("_zdb_xid");
                    }
                }
        );

        return abortedXids;
    }

    private static void ctid_and_xid_collect(BinaryDocValues ids, SortedSetDocValues ctids, SortedNumericDocValues xids, int doc, List<CtidXidInfo> collect, Map<BytesRef, List<Long>> lookup) {
        if (ctids == null || xids == null)
            return;

        ctids.setDocument(doc);
        xids.setDocument(doc);

        //
        // the order that these are retrieved is dictated by the fact that in {@link ZombodbBulkAction#handleInsertRequests}
        // we set the _id in 'updated' to the previous ctid, and the value of _zdb_udpated_ctid to the current document id
        //
        BytesRef prevctid = ids.get(doc);
        prevctid = new BytesRef("data#" + prevctid.utf8ToString().split("[#]")[1]);

        BytesRef id = new BytesRef("data#" + ctids.lookupOrd(ctids.nextOrd()).utf8ToString());

        long xid = xids.valueAt(0);

        collect.add(new CtidXidInfo(id, prevctid, xid));
        List<Long> exisitngXids = lookup.get(prevctid);
        if (exisitngXids == null)
            exisitngXids = new ArrayList<>();

        exisitngXids.add(xid);

        lookup.put(id, exisitngXids);
    }

    private static List<CtidXidInfo> findUpdatedCtids(IndexSearcher searcher, final Map<BytesRef, List<Long>> xidByCtid) throws IOException {
        final List<CtidXidInfo> updatedCtids = new ArrayList<>();

        searcher.search(new XConstantScoreQuery(SearchContext.current().filterCache().cache(new TermFilter(new Term("_type", "updated")))),
                new ZomboDBTermsCollector("_zdb_updated_ctid") {
                    BinaryDocValues ids;
                    SortedSetDocValues ctids;
                    SortedNumericDocValues xids;

                    @Override
                    public void collect(int doc) throws IOException {
                        ctid_and_xid_collect(ids, ctids, xids, doc, updatedCtids, xidByCtid);
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        ids = FieldCache.DEFAULT.getTerms(context.reader(), "_uid", false);
                        ctids = FieldCache.DEFAULT.getDocTermOrds(context.reader(), "_zdb_updated_ctid");
                        xids = context.reader().getSortedNumericDocValues("_updating_xid");
                    }
                }
        );

        return updatedCtids;
    }

    private static List<CtidXidInfo> findDeletedCtids(IndexSearcher searcher, final Map<BytesRef, List<Long>> xidByCtid) throws IOException {
        final List<CtidXidInfo> deletedCtids = new ArrayList<>();

        searcher.search(new XConstantScoreQuery(SearchContext.current().filterCache().cache(new TermFilter(new Term("_type", "deleted")))),
                new ZomboDBTermsCollector("_zdb_deleted_ctid") {
                    BinaryDocValues ids;
                    SortedSetDocValues ctids;
                    SortedNumericDocValues xids;

                    @Override
                    public void collect(int doc) throws IOException {
                        ctid_and_xid_collect(ids, ctids, xids, doc, deletedCtids, xidByCtid);
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        ids = FieldCache.DEFAULT.getTerms(context.reader(), "_uid", false);
                        ctids = FieldCache.DEFAULT.getDocTermOrds(context.reader(), "_zdb_deleted_ctid");
                        xids = context.reader().getSortedNumericDocValues("_deleting_xid");
                    }
                }
        );

        return deletedCtids;
    }


    static Map<Integer, FixedBitSet> determineVisibility(final Query query, final String field, final long myXid, final long xmin, final long xmax, final boolean all, final Set<Long> activeXids, IndexSearcher searcher) throws IOException {
        final Map<Integer, FixedBitSet> visibilityBitSets = new HashMap<>();

        final Map<BytesRef, List<Long>> deletedXidLookup = new HashMap<>();
        final Map<BytesRef, List<Long>> updatedXidLookup = new HashMap<>();
        final Set<Long> abortedXidLookup = new HashSet<>();

        final List<CtidXidInfo> deletedCtids = findDeletedCtids(searcher, deletedXidLookup);
        final List<CtidXidInfo> updatedCtids = findUpdatedCtids(searcher, updatedXidLookup);
        final List<BytesRef> abortedXids = findAbortedXids(searcher, myXid, abortedXidLookup);
        final Map<BytesRef, Long> previouslyUpdatedCtids = new HashMap<>();
        final List<Filter> filters = new ArrayList<>();

        if (!updatedCtids.isEmpty()) {
            filters.add(new TermsFilter("_uid", new ArrayList<>(
                    new AbstractList<BytesRef>() {
                        @Override
                        public BytesRef get(int index) {
                            return updatedCtids.get(index).prevctid;
                        }

                        @Override
                        public int size() {
                            return updatedCtids.size();
                        }
                    })));

            filters.add(new TermsFilter("_uid", new ArrayList<>(
                    new AbstractList<BytesRef>() {
                        @Override
                        public BytesRef get(int index) {
                            return updatedCtids.get(index).id;
                        }

                        @Override
                        public int size() {
                            return updatedCtids.size();
                        }
                    })));
        }

        if (!deletedCtids.isEmpty())
            filters.add(new TermsFilter("_uid", new ArrayList<>(
                    new AbstractList<BytesRef>() {
                        @Override
                        public BytesRef get(int index) {
                            return deletedCtids.get(index).id;
                        }

                        @Override
                        public int size() {
                            return deletedCtids.size();
                        }
                    }))
            );
        if (!abortedXids.isEmpty())
            filters.add(new TermsFilter("_xid", abortedXids));

        if (filters.isEmpty())
            return visibilityBitSets;

        for (CtidXidInfo info : updatedCtids)
            previouslyUpdatedCtids.put(info.prevctid, info.xid);

        //
        // build a map of {@link VisibilityInfo} objects by each _prev_ctid
        //
        // We use XConstantScoreQuery here so that we exclude deleted docs
        //
        final List<VisibilityInfo> visibilityList = new ArrayList<>();
        searcher.search(
                new XConstantScoreQuery(SearchContext.current().filterCache().cache(
                        filters.size() > 1 ?
                                new OrFilter(filters) :
                                filters.get(0)
                )),
                new ZomboDBTermsCollector(field) {
                    private BinaryDocValues ids;
                    private SortedNumericDocValues xids;
                    private SortedNumericDocValues sequence;
                    private int ord;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        if (xids == null)
                            return;

                        xids.setDocument(doc);
                        sequence.setDocument(doc);

                        long xid = xids.valueAt(0);
                        long seq = sequence.valueAt(0);
                        BytesRef id = BytesRef.deepCopyOf(ids.get(doc));

                        visibilityList.add(new VisibilityInfo(ord, maxdoc, doc, id, xid, seq));
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext context) throws IOException {
                        ids = FieldCache.DEFAULT.getTerms(context.reader(), "_uid", false);
                        xids = context.reader().getSortedNumericDocValues("_xid");
                        sequence = context.reader().getSortedNumericDocValues("_zdb_seq");
                        ord = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }
                }
        );

        if (visibilityList.isEmpty())
            return visibilityBitSets;

        InvisibilityMarker im = new InvisibilityMarker(visibilityBitSets);
        for (VisibilityInfo vi : visibilityList) {
            boolean isdelete = deletedXidLookup.containsKey(vi.id);
            boolean isupdate = previouslyUpdatedCtids.containsKey(vi.id);

            im.setVisibilityInfo(vi);

            // we just want to mark every possible version as invisible
            // this is only used for vacuum
            if (all) {
                im.invisible();
                continue;
            }


            //
            // general Postgres MVCC rules
            //

            // the row's transaction id is not ours and is otherwise is outside what we can currently view
            if (vi.xid != myXid && (vi.xid >= xmax || !(vi.xid < xmin) || activeXids.contains(vi.xid))) {
                im.invisible();
                continue;
            }


            //
            // ZDB specific rules
            //

            // the row's transaction aborted and it's not ours (ours is always considered aborted by default)
            if (abortedXidLookup.contains(vi.xid) && vi.xid != myXid) {
                im.invisible();
                continue;
            }


            //
            // UPDATE rules
            //

            if (isupdate) {
                // the row has been previously updated by our transaction
                if (previouslyUpdatedCtids.containsKey(vi.id) && previouslyUpdatedCtids.get(vi.id) == myXid) {
                    im.invisible();
                    continue;
                }

                // the row has been previously updated by a committed transaction (that by definition isn't ours)
                if (previouslyUpdatedCtids.containsKey(vi.id) && !abortedXidLookup.contains(previouslyUpdatedCtids.get(vi.id))) {
                    if (previouslyUpdatedCtids.get(vi.id) >= xmax || !(previouslyUpdatedCtids.get(vi.id) < xmin) /* || activeXids.contains(previouslyUpdatedCtids.get(vi.id))*/) {
                        // however, if the transaction of the previous one appears to be outside our view
                        // it actually means we *can* see it.  This is because Postgres thinks the transaction is still
                        // running (at the time we started the search), but ES thinks it's been committed
                        continue;
                    }
                    im.invisible();
                    continue;
                }

                continue;
            }


            //
            // DELETE rules
            //

            if (isdelete) {
                long xid = deletedXidLookup.get(vi.id).get(0);

                // the row was deleted by our transactions
                if (xid == myXid) {
                    im.invisible();
                    continue;
                }

                // the row was deleted by a committed transaction
                if (!abortedXidLookup.contains(xid)) {
                    if (xid >= xmax || !(xid < xmin) || activeXids.contains(xid)) {
                        continue;
                    }
                    im.invisible();
                    continue;
                }

                continue;
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

    private static boolean containsAny(Set<Long> a, Collection<Long> b) {
        for (Long b_v : b)
            if (a.contains(b_v))
                return true;
        return false;
    }
}
