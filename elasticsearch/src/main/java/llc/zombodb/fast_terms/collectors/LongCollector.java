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

import com.carrotsearch.hppc.LongArrayList;

public class LongCollector extends FastTermsCollector<long[]> {

    private final LongArrayList data = new LongArrayList();

    public LongCollector(String fieldname) {
        super(fieldname);
    }

    public long[] getData() {
        return data.buffer;
    }

    public int getDataCount() {
        return data.size();
    }

    @Override
    public void internal_collect(int doc) {
        switch (type) {
            case NUMERIC: {
                if (numeric == null)
                    return;

                long value = numeric.get(doc);
                data.add(value);
            }
            break;

            case SORTED_NUMERIC: {
                if (sortedNumeric == null)
                    return;

                sortedNumeric.setDocument(doc);
                int cnt = sortedNumeric.count();
                for (int i = 0; i < cnt; i++) {
                    long value = sortedNumeric.valueAt(i);
                    data.add(value);
                }
            }
            break;
        }
    }

}
