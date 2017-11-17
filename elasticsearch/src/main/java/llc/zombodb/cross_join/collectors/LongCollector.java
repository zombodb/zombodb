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

import com.carrotsearch.hppc.LongArrayList;

import java.util.Arrays;

class LongCollector extends CrossJoinCollector {

    private final LongArrayList array;

    LongCollector(String fieldname, LongArrayList longs) {
        super(fieldname);
        this.array = longs;
    }

    @Override
    public boolean accept(long value) {
        return Arrays.binarySearch(array.buffer, 0, array.elementsCount, value) >= 0;
    }
}
