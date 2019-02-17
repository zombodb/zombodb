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

import java.io.IOException;
import java.util.Arrays;

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

class StringCollector extends CrossJoinCollector {

    private final ObjectArrayList strings;

    StringCollector(LeafReaderContext context, String fieldname, ObjectArrayList<String> strings) throws IOException {
        super(context, fieldname);
        this.strings = strings;
    }

    @Override
    public boolean accept(BytesRef value) {
        return Arrays.binarySearch(strings.buffer, 0, strings.elementsCount, value.utf8ToString(), (o1, o2) -> {
            String a = (String) o1;
            String b = (String) o2;
            return a.compareTo(b);
        }) >= 0;
    }
}
