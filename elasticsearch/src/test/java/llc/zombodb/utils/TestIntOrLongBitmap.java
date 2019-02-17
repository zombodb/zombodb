package llc.zombodb.utils;


import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestIntOrLongBitmap {

    private static final int MANY = 327680;

    @Test
    public void testIt() throws Exception {
        for (int z=0; z<10; z++) {
            Random rnd = new Random();
            Set<Long> values = new HashSet<>();
            IntOrLongBitmap bitmap = new IntOrLongBitmap();

            long start = rnd.nextLong();

            for (int i = 0; i < MANY; i++) {
                long v = rnd.nextLong() + start;
                values.add(v);
                bitmap.add(v);
            }

            for (Long l : values) {
                if (!bitmap.contains(l))
                    throw new Exception("Couldn't find '" + l + "' in bitmap");
            }

            for (PrimitiveIterator.OfLong itr : bitmap.iterators()) {
                while (itr.hasNext()) {
                    long v = itr.nextLong();
                    if (!values.remove(v))
                        throw new Exception("Couldn't find '" + v + "' in values");
                }
            }

            if (!values.isEmpty())
                throw new Exception("values isn't empty: " + values);
        }
    }

    @Test
    public void test_764301484974400394() throws Exception {
        IntOrLongBitmap bitmap = new IntOrLongBitmap();

        bitmap.add(764301484974400394L);
        assertTrue(bitmap.contains(764301484974400394L));
    }
}
