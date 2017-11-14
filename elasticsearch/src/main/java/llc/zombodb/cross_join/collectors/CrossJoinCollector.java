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
package llc.zombodb.cross_join.collectors;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import llc.zombodb.visibility_query.ZomboDBTermsCollector;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class CrossJoinCollector extends ZomboDBTermsCollector {

    private final String fieldname;
    private Map<Integer, BitSet> bitsets = new HashMap<>();
    private int contextOrd;
    private int maxdoc;

    private DocValuesType docType;
    private SortedNumericDocValues sortedNumeric;
    private NumericDocValues numericDocValues;
    private SortedSetDocValues sortedSet;
    private SortedDocValues sortedDocValues;

    public static CrossJoinCollector create(String fieldname, LongArrayList longs) {
        return new LongCollector(fieldname, longs);
    }

    public static CrossJoinCollector create(String fieldname, IntArrayList ints) {
        return new IntCollector(fieldname, ints);
    }

    public static CrossJoinCollector create(String fieldname, ObjectArrayList<String> strings) {
        return new StringCollector(fieldname, strings);
    }

    CrossJoinCollector(String fieldname) {
        this.fieldname = fieldname;
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    public Map<Integer, BitSet> getBitsets() {
        return bitsets;
    }

    private void setBit(int doc) {
        BitSet bits = bitsets.get(contextOrd);
        if (bits == null)
            bitsets.put(contextOrd, bits = new FixedBitSet(maxdoc));
        bits.set(doc);
    }

    @Override
    public void collect(int doc) throws IOException {
        switch (docType) {
            case NUMERIC: {
                if (numericDocValues == null)
                    return;

                if (accept(numericDocValues.get(doc))) {
                    setBit(doc);
                }
            } break;

            case SORTED_NUMERIC: {
                if (sortedNumeric == null)
                    return;

                sortedNumeric.setDocument(doc);
                for (int i = 0; i < sortedNumeric.count(); i++) {
                    if (accept(sortedNumeric.valueAt(i))) {
                        setBit(doc);
                        break;
                    }
                }
            } break;

            case SORTED: {
                if (sortedDocValues == null)
                    return;

                if (accept(sortedDocValues.get(doc))) {
                    setBit(doc);
                }
            } break;

            case SORTED_SET: {
                if (sortedSet == null)
                    return;

                sortedSet.setDocument(doc);
                for (long ord = sortedSet.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = sortedSet.nextOrd()) {
                    if (accept(sortedSet.lookupOrd(ord))) {
                        setBit(doc);
                        break;
                    }
                }
            } break;

        }
    }

    public boolean accept(long value) { return false; }
    public boolean accept(BytesRef value) { return false; }

    @Override
    protected final void doSetNextReader(LeafReaderContext context) throws IOException {
        contextOrd = context.ord;
        maxdoc = context.reader().maxDoc();

        sortedNumeric = context.reader().getSortedNumericDocValues(fieldname);
        if (sortedNumeric != null) {
            docType = DocValuesType.SORTED_NUMERIC;
            return;
        }

        numericDocValues = context.reader().getNumericDocValues(fieldname);
        if (numericDocValues != null) {
            docType = DocValuesType.NUMERIC;
            return;
        }

        sortedDocValues = context.reader().getSortedDocValues(fieldname);
        if (sortedDocValues != null) {
            docType = DocValuesType.SORTED;
            return;
        }

        sortedSet = context.reader().getSortedSetDocValues(fieldname);
        if (sortedSet != null) {
            docType = DocValuesType.SORTED_SET;
            return;
        }

        docType = DocValuesType.NONE;
    }
}
