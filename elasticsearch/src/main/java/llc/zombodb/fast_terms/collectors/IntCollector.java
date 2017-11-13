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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

public class IntCollector extends FastTermsCollector<int[]> {
    private SortedNumericDocValues sorted;
    private NumericDocValues numeric;

    private IntArrayList data = new IntArrayList();

    public IntCollector(String fieldname) {
        super(fieldname);
    }

    public int[] getData() {
        return data.buffer;
    }

    public int getDataCount() {
        return data.size();
    }

    @Override
    public void internal_collect(int doc) throws IOException {
        switch (type) {
            case NUMERIC: {
                if (numeric == null)
                    return;

                data.add((int) numeric.get(doc));
            }
            break;

            case SORTED_NUMERIC: {
                if (sortedNumeric == null)
                    return;

                sortedNumeric.setDocument(doc);
                int cnt = sortedNumeric.count();
                for (int i = 0; i < cnt; i++) {
                    data.add((int) sortedNumeric.valueAt(i));
                }
            }
            break;
        }
    }
}
