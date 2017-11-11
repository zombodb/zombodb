package llc.zombodb.fast_terms.collectors;

import llc.zombodb.visibility_query.ZomboDBTermsCollector;
import org.apache.lucene.index.*;

import java.io.IOException;

public abstract class FastTermsCollector<T> extends ZomboDBTermsCollector {

    protected DocValuesType type;
    private final String fieldname;

    protected SortedNumericDocValues sortedNumeric;
    protected NumericDocValues numeric;
    protected SortedSetDocValues sortedSet;
    protected SortedDocValues sorted;

    public FastTermsCollector(String fieldname) {
        this.fieldname = fieldname;
    }

    public abstract T getData();

    public abstract int getDataCount();

    protected abstract void internal_collect(int doc) throws IOException;

    @Override
    public final void collect(int doc) throws IOException {
        if (type == DocValuesType.NONE)
            return;

        internal_collect(doc);
    }

    public boolean hasNegatives() {
        return false;
    }

    @Override
    protected final void doSetNextReader(LeafReaderContext context) throws IOException {
        numeric = context.reader().getNumericDocValues(fieldname);
        if (numeric != null) {
            type = DocValuesType.NUMERIC;
            return;
        }

        sortedNumeric = context.reader().getSortedNumericDocValues(fieldname);
        if (sortedNumeric != null) {
            type = DocValuesType.SORTED_NUMERIC;
            return;
        }

        sorted = context.reader().getSortedDocValues(fieldname);
        if (sorted != null) {
            type = DocValuesType.SORTED;
            return;
        }

        sortedSet = context.reader().getSortedSetDocValues(fieldname);
        if (sortedSet != null) {
            type = DocValuesType.SORTED_SET;
            return;
        }

        type = DocValuesType.NONE;
    }

}
