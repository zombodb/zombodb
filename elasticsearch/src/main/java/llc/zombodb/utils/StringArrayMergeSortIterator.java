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
package llc.zombodb.utils;

public class StringArrayMergeSortIterator {
    // Thanks, @ShitalShah from https://stackoverflow.com/a/31310853 for the inspiration

    private final Object[][] arrays;
    private final int[] counters;
    private final int[] lengths;
    private final int finalTotal;
    private int total;

    private String pushback;

    public StringArrayMergeSortIterator(Object[][] arrays, int[] lengths) {
        this.arrays = arrays;
        this.counters = new int[arrays.length];
        this.lengths = lengths;
        for (int l : lengths)
            total += l;
        this.finalTotal = total;
    }

    public void push(String value) {
        pushback = value;
    }

    public String next() {
        if (pushback != null) {
            String tmp = pushback;
            pushback = null;
            return tmp;
        }

        --total;

        // find first array that we haven't exhausted
        // to assume it'll have the smallest value
        int smallest = 0;
        while (counters[smallest] >= lengths[smallest])
            smallest++;

        for (int i = smallest+1; i < counters.length; i++) {
            if (counters[i] < lengths[i] &&   // this one is exhausted
                String.valueOf(arrays[i][counters[i]]).compareTo(String.valueOf(arrays[smallest][counters[smallest]])) <= 0 // this one is smaller
            ) {
                smallest = i;
            }
        }

        return String.valueOf(arrays[smallest][counters[smallest]++]);
    }

    public boolean hasNext() {
        return total > 0 || pushback != null;
    }

    public int getTotal() {
        return finalTotal;
    }
}
