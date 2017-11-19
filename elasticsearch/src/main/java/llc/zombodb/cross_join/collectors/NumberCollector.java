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

import llc.zombodb.utils.NumberArrayLookup;

class NumberCollector extends CrossJoinCollector {

    private final NumberArrayLookup[] bitsets;

    NumberCollector(String fieldname, NumberArrayLookup[] bitsets) {
        super(fieldname);
        this.bitsets = bitsets;
    }

    @Override
    public boolean accept(long value) {
        for (NumberArrayLookup bitset : bitsets) {
            if (bitset.get(value))
                return true;
        }
        return false;
    }
}
