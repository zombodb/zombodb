package com.tcdi.zombodb.query;

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
