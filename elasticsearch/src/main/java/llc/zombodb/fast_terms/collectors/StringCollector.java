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

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.index.SortedSetDocValues;

public class StringCollector extends FastTermsCollector<Object[]> {
    private final ObjectArrayList<String> data = new ObjectArrayList<>();


    public StringCollector(String fieldname) {
        super(fieldname);
    }

    public Object[] getData() {
        return data.buffer;
    }

    @Override
    public int getDataCount() {
        return data.size();
    }

    @Override
    public void internal_collect(int doc) {
        switch (type) {
            case SORTED: {
                if (sorted == null)
                    return;

                data.add(sorted.get(doc).utf8ToString());
            }
            break;

            case SORTED_SET: {
                if (sortedSet == null)
                    return;

                sortedSet.setDocument(doc);
                for (long ord = sortedSet.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = sortedSet.nextOrd()) {
                    data.add(sortedSet.lookupOrd(ord).utf8ToString());
                }
            }
            break;
        }
    }

}
