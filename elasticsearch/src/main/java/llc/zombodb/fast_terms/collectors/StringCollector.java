package llc.zombodb.fast_terms.collectors;

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

public class StringCollector extends FastTermsCollector<Object[]> {
    private ObjectArrayList<String> data = new ObjectArrayList<>();


    public StringCollector(String fieldname) {
        super(fieldname);
    }

    public Object[] getData() {
        return data.buffer;
    }

    @Override
    public int getDataCount() {
        return data.size();
    }

    @Override
    public void internal_collect(int doc) throws IOException {
        switch (type) {
            case SORTED: {
                if (sorted == null)
                    return;

                data.add(sorted.get(doc).utf8ToString());
            }
            break;

            case SORTED_SET: {
                if (sortedSet == null)
                    return;

                sortedSet.setDocument(doc);
                for (long ord = sortedSet.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = sortedSet.nextOrd()) {
                    data.add(sortedSet.lookupOrd(ord).utf8ToString());
                }
            }
            break;
        }
    }

}
