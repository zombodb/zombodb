package llc.zombodb.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestXXXArrayMergeSortIterator {

    @Test
    public void testString() throws Exception {
        for (int cnt=0; cnt<1000; cnt++) {
            Random rnd = new Random();
            int MAX = 250;

            String[][] strings = new String[10][];
            int[] lengths = new int[strings.length];
            for (int i = 0; i < strings.length; i++) {
                int many = Math.abs(rnd.nextInt()) % MAX;

                String[] values = new String[many];
                lengths[i] = values.length;
                for (int j = 0; j < many; j++)
                    values[j] = String.valueOf(rnd.nextDouble());
                Arrays.sort(values);
                strings[i] = values;
            }

            StringArrayMergeSortIterator itr = new StringArrayMergeSortIterator(strings, lengths);
            String prev = null;
            boolean first = true;
            int processed = 0, total = itr.getTotal();
            while (itr.hasNext()) {
                String value = itr.next();
                if (!first) {
                    if (!(value.compareTo(prev) >= 0)) {
                        System.err.println("NOT SORTED: current=" + value + ", prev=" + prev);
                    }
                    assertTrue(value.compareTo(prev) >= 0);
                }

                prev = value;
                first = false;
                processed++;
            }

            assertTrue(total == processed);
        }
    }
}
