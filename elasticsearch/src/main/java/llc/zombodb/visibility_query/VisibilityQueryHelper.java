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
package llc.zombodb.visibility_query;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.*;

final class VisibilityQueryHelper {

    /**
     * collect all the _zdb_xid values in the "aborted" type as both a set of Longs and as a list of BytesRef
     * for filtering in #determineVisibility
     */
    private static void collectAbortedXids(IndexSearcher searcher, final Set<Long> abortedXids) throws IOException {
        searcher.search(new ConstantScoreQuery(new TermQuery(new Term("_type", "aborted"))),
                new ZomboDBTermsCollector() {
                    SortedNumericDocValues _zdb_xid;

                    @Override
                    public void collect(int doc) throws IOException {
                        _zdb_xid.setDocument(doc);

                        for (int i=0; i<_zdb_xid.count(); i++)
                            abortedXids.add(_zdb_xid.valueAt(i));
                    }

                    @Override
                    protected void doSetNextReader(LeafReaderContext context) throws IOException {
                        _zdb_xid = context.reader().getSortedNumericDocValues("_zdb_xid");
                    }
                }
        );
    }

    /**
     * Collect all the "xmax" docs that exist in the shard we're running on.
     *
     * Depending on the state of the table, there can potentially be thousands or even millions
     * of these that we have to process, so we try really hard to limit the amount of work we
     * need to do for each one
     */
    private static void collectMaxes(IndexSearcher searcher, final Map<HeapTuple, HeapTuple> tuples, final IntSet dirtyBlocks) throws IOException {
        abstract class Collector extends ZomboDBTermsCollector {
            final ByteArrayDataInput in = new ByteArrayDataInput();
            BinaryDocValues _zdb_encoded_tuple;

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                _zdb_encoded_tuple = context.reader().getBinaryDocValues("_zdb_encoded_tuple");
            }
        }

        searcher.search(new ConstantScoreQuery(new TermQuery(new Term("_type", "xmax"))),
                new Collector() {
                    @Override
                    public void collect(int doc) throws IOException {
                        HeapTuple ctid = new HeapTuple(_zdb_encoded_tuple.get(doc), false, in);
                        tuples.put(ctid, ctid);
                        dirtyBlocks.add(ctid.blockno);
                    }
                }
        );
    }

    static Map<Integer, FixedBitSet> determineVisibility(final long myXid, final long myXmin, final long myXmax, final int myCommand, final Set<Long> activeXids, IndexSearcher searcher) throws IOException {
        final IntSet dirtyBlocks = new IntHashSet();
        final Map<HeapTuple, HeapTuple> modifiedTuples = new HashMap<>();
        final Set<Long> abortedXids = new HashSet<>();

        collectAbortedXids(searcher, abortedXids);
        collectMaxes(searcher, modifiedTuples, dirtyBlocks);

        final boolean just_get_everything = modifiedTuples.size() >= searcher.getIndexReader().maxDoc()/3;

        final List<Query> filters = new ArrayList<>();
        if (just_get_everything) {
            // if the number of docs with xmax values is at least 1/3 of the total docs
            // just go ahead and ask for everything.  This is much faster than asking
            // lucene to parse and lookup tens of thousands (or millions!) of individual
            // _uid values
            filters.add(new MatchAllDocsQuery());
        } else {
            // just look at all the docs on the blocks we've identified as dirty
            if (!dirtyBlocks.isEmpty())
                filters.add(IntPoint.newSetQuery("_zdb_blockno", dirtyBlocks.toArray()));

            // we also need to examine docs that might be aborted or inflight on non-dirty pages
            if (!activeXids.isEmpty())
                filters.add(LongPoint.newSetQuery("_xmin", activeXids));
            if (!abortedXids.isEmpty())
                filters.add(LongPoint.newSetQuery("_xmin", abortedXids));

            filters.add(LongPoint.newRangeQuery("_xmin", myXmin, Long.MAX_VALUE));
        }

        //
        // find all "data" docs that we think we might need to examine for visibility
        // given the set of filters above, this is likely to be over-inclusive
        // but that's okay because it's cheaper to find and examine more docs
        // than it is to use TermsFilters with very long lists of _ids
        //
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        BooleanQuery.Builder builder2 = new BooleanQuery.Builder();

        for (Query q : filters)
            builder2.add(q, BooleanClause.Occur.SHOULD);
        builder.add(new TermQuery(new Term("_type", "data")), BooleanClause.Occur.MUST);
        builder.add(builder2.build(), BooleanClause.Occur.MUST);

        final Map<Integer, FixedBitSet> visibilityBitSets = new HashMap<>();
        searcher.search(new ConstantScoreQuery(builder.build()),
                new ZomboDBTermsCollector() {
                    private final ByteArrayDataInput in = new ByteArrayDataInput();
                    private BinaryDocValues _zdb_encoded_tuple;
                    private int contextOrd;
                    private int maxdoc;

                    @Override
                    public void collect(int doc) throws IOException {
                        if (_zdb_encoded_tuple == null)
                            return;
                        HeapTuple ctid = new HeapTuple(_zdb_encoded_tuple.get(doc), true, in);  // from "data"
                        HeapTuple ctidWithXmax = modifiedTuples.get(ctid);  // from "xmax"

                        // get all the xmin/xmax, cmin/cmax values we need to determine visibility below
                        long xmin = ctid.xmin;
                        int cmin = ctid.cmin;
                        boolean xmax_is_null = ctidWithXmax == null;
                        long xmax = -1;
                        int cmax = -1;

                        if (!xmax_is_null) {
                            xmax = ctidWithXmax.xmax;
                            cmax = ctidWithXmax.cmax;
                        }

                        // we can only consider transactions as committed or aborted if they're not outside
                        // our current snapshot's xmax (myXmax) and aren't otherwise considered active or aborted in some way

                        boolean xmin_is_committed = !(xmin >= myXmax) && !activeXids.contains(xmin) && !abortedXids.contains(xmin);
                        boolean xmax_is_committed = !xmax_is_null && !(xmax >= myXmax) && !activeXids.contains(xmax) && !abortedXids.contains(xmax);


                        //
                        // the logic below is taken from Postgres 9.3's "tqual.c#HeapTupleSatifiesNow()"
                        //

                        /*
                         * HeapTupleSatisfiesNow
                         *		True iff heap tuple is valid "now".
                         *
                         *	Here, we consider the effects of:
                         *		all committed transactions (as of the current instant)
                         *		previous commands of this transaction
                         *
                         * Note we do _not_ include changes made by the current command.  This
                         * solves the "Halloween problem" wherein an UPDATE might try to re-update
                         * its own output tuples, http://en.wikipedia.org/wiki/Halloween_Problem.
                         *
                         * Note:
                         *		Assumes heap tuple is valid.
                         *
                         * The satisfaction of "now" requires the following:
                         *
                         * ((Xmin == my-transaction &&				inserted by the current transaction
                         *	 Cmin < my-command &&					before this command, and
                         *	 (Xmax is null ||						the row has not been deleted, or
                         *	  (Xmax == my-transaction &&			it was deleted by the current transaction
                         *	   Cmax >= my-command)))				but not before this command,
                         * ||										or
                         *	(Xmin is committed &&					the row was inserted by a committed transaction, and
                         *		(Xmax is null ||					the row has not been deleted, or
                         *		 (Xmax == my-transaction &&			the row is being deleted by this transaction
                         *		  Cmax >= my-command) ||			but it's not deleted "yet", or
                         *		 (Xmax != my-transaction &&			the row was deleted by another transaction
                         *		  Xmax is not committed))))			that has not been committed
                         *
                         */
                        if (
                                !(
                                        (xmin == myXid && cmin < myCommand && (xmax_is_null || (xmax == myXid && cmax >= myCommand)))
                                                ||
                                        (xmin_is_committed && (xmax_is_null || (xmax == myXid && cmax >= myCommand) || (xmax != myXid && !xmax_is_committed)))
                                )
                            ) {
                            // it's not visible to us
                            FixedBitSet visibilityBitset = visibilityBitSets.get(contextOrd);
                            if (visibilityBitset == null)
                                visibilityBitSets.put(contextOrd, visibilityBitset = new FixedBitSet(maxdoc));
                            visibilityBitset.set(doc);
                        }
                    }

                    @Override
                    protected void doSetNextReader(LeafReaderContext context) throws IOException {
                        _zdb_encoded_tuple = context.reader().getBinaryDocValues("_zdb_encoded_tuple");
                        contextOrd = context.ord;
                        maxdoc = context.reader().maxDoc();
                    }

                    @Override
                    public boolean needsScores() {
                        return false;
                    }
                }
        );

        return visibilityBitSets;
    }
}
