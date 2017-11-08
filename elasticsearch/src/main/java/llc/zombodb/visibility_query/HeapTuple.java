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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

class HeapTuple implements Comparable<HeapTuple> {
    final int blockno, offno;
    long xmin;
    int cmin;
    long xmax;
    int cmax;

    private final int hash;

    /* for unit testing */
    HeapTuple(int blockno, int offno) {
        this.blockno = blockno;
        this.offno = offno;

        this.hash = blockno + (31 * offno);
    }

    HeapTuple(BytesRef bytes, boolean isxmin, ByteArrayDataInput in) {
        // lucene prefixes binary terms with a header of two variable length ints.
        // because we know how our binary data is constructed (it could never be
        // more than 18 bytes) we can blindly assume that the header length is 2 bytes.
        // 1 byte for the number of items and 1 byte for the first/only item's byte
        // length, neither of which we need
        in.reset(bytes.bytes, 2, bytes.length-2);

        blockno = in.readVInt();
        offno = in.readVInt();
        if (in.getPosition() < bytes.length) {
            // more bytes, so we also have xmax and cmax to read
            if (isxmin) {
                xmin = in.readVLong();
                cmin = in.readVInt();
            } else {
                xmax = in.readVLong();
                cmax = in.readVInt();
            }
        }

        hash = blockno + (31 * offno);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        assert(obj instanceof HeapTuple);
        HeapTuple other = (HeapTuple) obj;
        return this.blockno == other.blockno && this.offno == other.offno;
    }

    @Override
    public int compareTo(HeapTuple other) {
        return this.blockno < other.blockno ? -1 : this.blockno > other.blockno ? 1 : this.offno - other.offno;
    }
}
