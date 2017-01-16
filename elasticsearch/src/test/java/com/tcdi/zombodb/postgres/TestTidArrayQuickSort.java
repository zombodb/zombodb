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
    public void testIt() throws Exception {

        for (int many=0; many<5000; many++) {
            byte[] array = sort(many);

            for (int i=1; i<many; i++) {
                int prev = Utils.decodeInteger(array, 13 + (i-1)*10);
                int curr = Utils.decodeInteger(array, 13 + i*10);
                assertTrue("many=" + many + "; prev=" + prev + ", curr=" + curr, prev <= curr);
            }
        }
    }

    private byte[] sort(int many) {
        byte[] array = new byte[1 + 8 + 4 + (many * 10)];    // NULL + totalhits + maxscore + (many * (sizeof(int4)+sizeof(int2)+sizeof(float4)))
        int offset = 0;

        array[0] = 0;
        offset++;
        offset += Utils.encodeLong(many, array, offset);
        offset += Utils.encodeFloat(32768, array, offset); // max_score

        int first_byte = offset;
        for (int i=0; i<many; i++) {
            int blockno = rnd.nextInt();
            char offno = (char) rnd.nextInt();
            float score = rnd.nextFloat();

            offset += Utils.encodeInteger(blockno, array, offset);
            offset += Utils.encodeCharacter(offno, array, offset);
            offset += Utils.encodeFloat(score, array, offset);
        }

        new PostgresTIDResponseAction.TidArrayQuickSort().quickSort(array, first_byte, 0, many-1);

        return array;
    }
}
