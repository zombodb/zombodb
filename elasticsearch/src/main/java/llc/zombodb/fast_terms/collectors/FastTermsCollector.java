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
package llc.zombodb.fast_terms.collectors;

import llc.zombodb.visibility_query.ZomboDBTermsCollector;
import org.apache.lucene.index.*;

import java.io.IOException;

public abstract class FastTermsCollector<T> extends ZomboDBTermsCollector {

    DocValuesType type;
    private final String fieldname;

    SortedNumericDocValues sortedNumeric;
    NumericDocValues numeric;
    SortedSetDocValues sortedSet;
    SortedDocValues sorted;

    FastTermsCollector(String fieldname) {
        this.fieldname = fieldname;
    }

    public abstract T getData();

    public abstract int getDataCount();

    protected abstract void internal_collect(int doc);

    @Override
    public final void collect(int doc) throws IOException {
        if (type == DocValuesType.NONE)
            return;

        internal_collect(doc);
    }

    @Override
    protected final void doSetNextReader(LeafReaderContext context) throws IOException {
        numeric = context.reader().getNumericDocValues(fieldname);
        if (numeric != null) {
            type = DocValuesType.NUMERIC;
            return;
        }

        sortedNumeric = context.reader().getSortedNumericDocValues(fieldname);
        if (sortedNumeric != null) {
            type = DocValuesType.SORTED_NUMERIC;
            return;
        }

        sorted = context.reader().getSortedDocValues(fieldname);
        if (sorted != null) {
            type = DocValuesType.SORTED;
            return;
        }

        sortedSet = context.reader().getSortedSetDocValues(fieldname);
        if (sortedSet != null) {
            type = DocValuesType.SORTED_SET;
            return;
        }

        type = DocValuesType.NONE;
    }

}
