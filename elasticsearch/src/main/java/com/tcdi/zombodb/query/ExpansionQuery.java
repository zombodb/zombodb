package com.tcdi.zombodb.query;

import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ZomboDBJoinUtil;
import org.apache.lucene.search.join.ZomboDBTermsQuery;
import org.apache.lucene.util.*;
import org.elasticsearch.common.hppc.IntArrayList;
import org.elasticsearch.common.hppc.LongIntMap;
import org.elasticsearch.common.hppc.LongIntOpenHashMap;
import org.elasticsearch.common.hppc.LongObjectMap;
import org.elasticsearch.common.hppc.cursors.IntCursor;
import org.elasticsearch.common.hppc.cursors.LongIntCursor;
import org.elasticsearch.common.hppc.cursors.LongObjectCursor;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ExpansionQuery extends Query {

    private final String fieldname;
    private final Query subquery;

    public ExpansionQuery(String fieldname, Query subquery) {
        this.fieldname = fieldname;
        this.subquery = subquery;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {

        final LongObjectMap<IntArrayList> docs = ZomboDBJoinUtil.createJoinQuery(fieldname, subquery, reader);
        if (docs == null)
            return new MatchNoDocsQuery();

        List<BytesRef> bytes = new ArrayList<>();
        for (LongObjectCursor<IntArrayList> cursor : docs) {

            if (cursor.value.size() > 1) {
                BytesRefBuilder bytesRef = new BytesRefBuilder();
                NumericUtils.longToPrefixCoded(cursor.key, 0, bytesRef);  // 0 because of exact match
                bytes.add(bytesRef.get());
            }
        }

        if (bytes.size() == 0)
            return new MatchNoDocsQuery();

        return new ConstantScoreQuery(new TermsFilter(fieldname, bytes));
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subquery.extractTerms(terms);
    }

    @Override
    public String toString(String field) {
        return "expansion(" + fieldname + ", " + subquery.toString(field) + ")";
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = hash * 31 + fieldname.hashCode();
        hash = hash * 31 + subquery.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        assert obj instanceof ExpansionQuery;
        ExpansionQuery eq = (ExpansionQuery) obj;

        return this.fieldname.equals(eq.fieldname) && this.subquery.equals(eq.subquery);
    }
}
