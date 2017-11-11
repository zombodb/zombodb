package llc.zombodb.fast_terms.collectors;

import com.carrotsearch.hppc.LongArrayList;

import java.io.IOException;

public class LongCollector extends FastTermsCollector<long[]> {

    private LongArrayList data = new LongArrayList();
    private boolean hasNegatives = false;

    public LongCollector(String fieldname) {
        super(fieldname);
    }

    public long[] getData() {
        return data.buffer;
    }

    public int getDataCount() {
        return data.size();
    }

    @Override
    public boolean hasNegatives() {
        return hasNegatives;
    }

    @Override
    public void internal_collect(int doc) throws IOException {
        switch (type) {
            case NUMERIC: {
                if (numeric == null)
                    return;

                long value = numeric.get(doc);
                data.add(value);
                if (!hasNegatives && value < 0)
                    hasNegatives = true;
            }
            break;

            case SORTED_NUMERIC: {
                if (sortedNumeric == null)
                    return;

                sortedNumeric.setDocument(doc);
                int cnt = sortedNumeric.count();
                for (int i = 0; i < cnt; i++) {
                    long value = sortedNumeric.valueAt(i);
                    data.add(value);
                    if (!hasNegatives && value < 0)
                        hasNegatives = true;
                }
            }
            break;
        }
    }

}
