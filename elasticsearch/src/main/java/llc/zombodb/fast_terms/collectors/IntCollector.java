package llc.zombodb.fast_terms.collectors;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

public class IntCollector extends FastTermsCollector<int[]> {
    private SortedNumericDocValues sorted;
    private NumericDocValues numeric;

    private IntArrayList data = new IntArrayList();

    public IntCollector(String fieldname) {
        super(fieldname);
    }

    public int[] getData() {
        return data.buffer;
    }

    public int getDataCount() {
        return data.size();
    }

    @Override
    public void internal_collect(int doc) throws IOException {
        switch (type) {
            case NUMERIC: {
                if (numeric == null)
                    return;

                data.add((int) numeric.get(doc));
            }
            break;

            case SORTED_NUMERIC: {
                if (sortedNumeric == null)
                    return;

                sortedNumeric.setDocument(doc);
                int cnt = sortedNumeric.count();
                for (int i = 0; i < cnt; i++) {
                    data.add((int) sortedNumeric.valueAt(i));
                }
            }
            break;
        }
    }
}
