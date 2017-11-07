package com.tcdi.zombodb.postgres;

import com.tcdi.zombodb.query_parser.utils.Utils;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Created by e_ridge on 12/28/16.
 */
public class TestTidArrayQuickSort {
    private static Random rnd = new Random(0);

    @Test
    public void testIt_WithScores() throws Exception {
        int size = 10;
        for (int many=0; many<5000; many++) {
            byte[] array = gen_data(many, size);

            for (int i=1; i<many; i++) {
                int prev = Utils.decodeInteger(array, 13 + (i-1)*size);
                int curr = Utils.decodeInteger(array, 13 + i*size);
                assertTrue("many=" + many + "; size=" + size + "; prev=" + prev + ", curr=" + curr, prev <= curr);
                assertTrue("many=" + many + "; size=" + size + "; prev=" + prev + ", curr=" + curr, prev >= 32768);
                assertTrue("many=" + many + "; size=" + size + "; prev=" + prev + ", curr=" + curr, curr >= 32768);
            }
        }
    }

    @Test
    public void testIt_WithoutScores() throws Exception {
        int size = 6;
        for (int many=0; many<5000; many++) {
            byte[] array = gen_data(many, size);

            for (int i=1; i<many; i++) {
                int prev = Utils.decodeInteger(array, 9 + (i-1)*size);
                int curr = Utils.decodeInteger(array, 9 + i*size);
                assertTrue("many=" + many + "; size=" + size + "; prev=" + prev + ", curr=" + curr, prev <= curr);
                assertTrue("many=" + many + "; size=" + size + "; prev=" + prev + ", curr=" + curr, prev >= 32768);
                assertTrue("many=" + many + "; size=" + size + "; prev=" + prev + ", curr=" + curr, curr >= 32768);
            }
        }
    }

    private byte[] gen_data(int many, int size) {
        byte[] array = new byte[1 + 8 + (size == 10 ? 4 : 0) + (many * size)];    // NULL + totalhits + maxscore + (many * (sizeof(int4)+sizeof(int2)+sizeof(float4)))
        int offset = 0;

        array[0] = 0;
        offset++;
        offset += Utils.encodeLong(many, array, offset);

        if (size == 10)
            offset += Utils.encodeFloat(1, array, offset); // max_score

        int first_byte = offset;
        for (int i=0; i<many; i++) {
            int blockno = 32768 + (Math.abs(rnd.nextInt()) % 32768);
            char offno = (char) + Math.abs(rnd.nextInt());
            float score = Math.abs(rnd.nextFloat());

            offset += Utils.encodeInteger(blockno, array, offset);
            offset += Utils.encodeCharacter(offno, array, offset);

            if (size == 10)
                offset += Utils.encodeFloat(score, array, offset);
        }

        new PostgresTIDResponseAction.TidArrayQuickSort().quickSort(array, first_byte, 0, many-1, size);

        return array;
    }
}
