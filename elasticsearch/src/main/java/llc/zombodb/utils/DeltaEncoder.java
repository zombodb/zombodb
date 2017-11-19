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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class DeltaEncoder {

    static void encode_longs_as_deltas(long[] longs, int len, StreamOutput out) throws IOException {
        out.writeVInt(len);
        if (len > 0) {
            boolean hasNegative = longs[0] < 0;
            out.writeBoolean(hasNegative);

            if (hasNegative) {
                out.writeZLong(longs[0]);
                for (int i = 1; i < len; i++) {
                    out.writeZLong(longs[i] - longs[i - 1]);
                }
            } else {
                out.writeVLong(longs[0]);
                for (int i = 1; i < len; i++) {
                    out.writeVLong(longs[i] - longs[i - 1]);
                }
            }
        }
    }

    static long[] decode_longs_from_deltas(StreamInput in) throws IOException {
        int len = in.readVInt();
        long[] longs = new long[len];

        if (len > 0) {
            boolean hasNegative = in.readBoolean();

            if (hasNegative) {
                longs[0] = in.readZLong();
                for (int i = 1; i < len; i++) {
                    longs[i] = longs[i - 1] + in.readZLong();
                }
            } else {
                longs[0] = in.readVLong();
                for (int i = 1; i < len; i++) {
                    longs[i] = longs[i - 1] + in.readVLong();
                }
            }
        }
        return longs;
    }
}
