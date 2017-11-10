package llc.zombodb.fast_terms.collectors;

import com.carrotsearch.hppc.LongArrayList;

import java.io.IOException;

public class LongCollector extends FastTermsCollector<long[]> {

    private LongArrayList data = new LongArrayList();

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
    public void internal_collect(int doc) throws IOException {
        switch (type) {
            case NUMERIC: {
                if (numeric == null)
                    return;

                data.add(numeric.get(doc));
            }
            break;

            case SORTED_NUMERIC: {
                if (sortedNumeric == null)
                    return;

                sortedNumeric.setDocument(doc);
                int cnt = sortedNumeric.count();
                for (int i = 0; i < cnt; i++) {
                    data.add(sortedNumeric.valueAt(i));
                }
            }
            break;
        }
    }

}
