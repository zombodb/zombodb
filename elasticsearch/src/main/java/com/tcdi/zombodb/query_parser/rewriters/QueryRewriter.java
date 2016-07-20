/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tcdi.zombodb.query_parser.rewriters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcdi.zombodb.query_parser.*;
import com.tcdi.zombodb.query_parser.QueryParser;
import com.tcdi.zombodb.query_parser.metadata.IndexMetadata;
import com.tcdi.zombodb.query_parser.metadata.IndexMetadataManager;
import com.tcdi.zombodb.query_parser.optimizers.ArrayDataOptimizer;
import com.tcdi.zombodb.query_parser.optimizers.IndexLinkOptimizer;
import com.tcdi.zombodb.query_parser.optimizers.TermAnalyzerOptimizer;
import com.tcdi.zombodb.query_parser.utils.EscapingStringTokenizer;
import com.tcdi.zombodb.query_parser.utils.Utils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

public abstract class QueryRewriter {

    public static class Factory {
        private static boolean IS_SIREN_AVAILABLE;

        static {
            try {
                IS_SIREN_AVAILABLE = Class.forName("solutions.siren.join.index.query.FilterJoinBuilder") != null;
                System.err.println("[zombodb] Using SIREn for join resolution");
            } catch (Exception e) {
                IS_SIREN_AVAILABLE = false;
                System.err.println("[zombodb] Using ZomboDB for join resolution");
            }
        }

        public static QueryRewriter create(Client client, String indexName, String searchPreference, String input, boolean doFullFieldDataLookup, boolean canDoSingleIndex) {
            if (IS_SIREN_AVAILABLE) {
                try {
                    Class clazz = Class.forName("com.tcdi.zombodb.query_parser.rewriters.SirenQueryRewriter");
                    Constructor ctor = clazz.getConstructor(Client.class, String.class, String.class, String.class, boolean.class, boolean.class);
                    return (QueryRewriter) ctor.newInstance(client, indexName, searchPreference, input, doFullFieldDataLookup, canDoSingleIndex);
                } catch (Exception e) {
                    throw new RuntimeException("Unable to construct SIREn-compatible QueryRewriter", e);
                }
            } else {
                return new ZomboDBQueryRewriter(client, indexName, searchPreference, input, doFullFieldDataLookup, canDoSingleIndex);
            }
        }
    }

    private enum DateHistogramIntervals {
        year, quarter, month, week, day, hour, minute, second
    }

    /* short for QueryBuilderFactory */
    private interface QBF {
        QBF DUMMY = new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                throw new QueryRewriteException("Should not get here");
            }
        };

        QueryBuilder b(QueryParserNode n);
    }

    /**
     * Container for range aggregation spec
     */
    static class RangeSpecEntry {
        public String key;
        public Double from;
        public Double to;
    }

    /**
     * Container for date range aggregation spec
     */
    static class DateRangeSpecEntry {
        public String key;
        public String from;
        public String to;
    }

    public static class QueryRewriteException extends RuntimeException {
        public QueryRewriteException(String message) {
            super(message);
        }

        public QueryRewriteException(Throwable cause) {
            super(cause);
        }

        public QueryRewriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final String DateSuffix = ".date";

    protected final Client client;
    protected final String searchPreference;
    protected final boolean doFullFieldDataLookup;
    protected final ASTQueryTree tree;

    private boolean _isBuildingAggregate = false;
    private boolean queryRewritten = false;

    private Map<String, String> arrayData;

    protected final IndexMetadataManager metadataManager;

    public QueryRewriter(Client client, String indexName, String input, String searchPreference, boolean doFullFieldDataLookup, boolean canDoSingleIndex) {
        this.client = client;
        this.searchPreference = searchPreference;
        this.doFullFieldDataLookup = doFullFieldDataLookup;

        metadataManager = new IndexMetadataManager(client, indexName);

        final StringBuilder newQuery = new StringBuilder(input.length());

        try {
            arrayData = Utils.extractArrayData(input, newQuery);

            QueryParser parser = new QueryParser(new StringReader(newQuery.toString()));
            tree = parser.parse(metadataManager, true);
        } catch (ParseException ioe) {
            throw new QueryRewriteException(ioe);
        }

        ASTAggregate aggregate = tree.getAggregate();
        ASTSuggest suggest = tree.getSuggest();
        boolean hasAgg = aggregate != null || suggest != null;
        if (hasAgg) {
            String fieldname = aggregate != null ? aggregate.getFieldname() : suggest.getFieldname();
            final ASTIndexLink indexLink = metadataManager.findField(fieldname);
            if (indexLink != metadataManager.getMyIndex()) {
                // change "myIndex" to that of the aggregate/suggest index
                // so that we properly expand() the queries to do the right things
                metadataManager.setMyIndex(indexLink);
            }
        }

        performOptimizations(client);

        if (!metadataManager.getMetadataForMyIndex().alwaysResolveJoins()) {
            if (canDoSingleIndex && !hasAgg && metadataManager.getUsedIndexes().size() == 1) {
                metadataManager.setMyIndex(metadataManager.getUsedIndexes().iterator().next());
            }
        }
    }

    /**
     * Subclasses can override if additional optimizations are necessary, but
     * they should definitely call {@link super.performOptimizations(Client)}
     */
    protected void performOptimizations(Client client) {
        new ArrayDataOptimizer(tree, metadataManager, arrayData).optimize();
        new IndexLinkOptimizer(client, this, tree, metadataManager).optimize();
        new TermAnalyzerOptimizer(client, metadataManager, tree).optimize();
    }

    public String dumpAsString() {
        return tree.dumpAsString();
    }

    public Map<String, ?> describedNestedObject(String fieldname) throws Exception {
        return metadataManager.describedNestedObject(fieldname);
    }

    public QueryBuilder rewriteQuery() {
        QueryBuilder qb = build(tree);
        queryRewritten = true;

        try {
            return applyExclusion(qb, getAggregateIndexName());
        } catch (Exception e) {
            return applyExclusion(qb, getSearchIndexName());
        }
    }

    public AbstractAggregationBuilder rewriteAggregations() {
        try {
            _isBuildingAggregate = true;
            return build(tree.getAggregate());
        } finally {
            _isBuildingAggregate = false;
        }
    }

    public boolean isAggregateNested() {
        return tree.getAggregate().isNested();
    }

    public SuggestBuilder.SuggestionBuilder rewriteSuggestions() {
        try {
            _isBuildingAggregate = true;
            return build(tree.getSuggest());
        } finally {
            _isBuildingAggregate = false;
        }
    }

    public String getAggregateIndexName() {
        if (tree.getAggregate() != null)
            return metadataManager.findField(tree.getAggregate().getFieldname()).getIndexName();
        else if (tree.getSuggest() != null)
            return metadataManager.findField(tree.getSuggest().getFieldname()).getIndexName();
        else
            throw new QueryRewriteException("Cannot figure out which index to use for aggregation");
    }

    public String getAggregateFieldName() {
        return getAggregateFieldName(tree.getAggregate());

    }

    public String getAggregateFieldName(ASTAggregate agg) {
        String fieldname = agg.getFieldname();
        IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        return maybeStripFieldname(fieldname, md);
    }

    public String getSuggestFieldName() {
        String fieldname = tree.getSuggest().getFieldname();
        IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        return maybeStripFieldname(fieldname, md);
    }

    private String maybeStripFieldname(String fieldname, IndexMetadata md) {
        if (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            if (base.equals(md.getLink().getFieldname()))   // strip base fieldname becase it's in a named index, not a json field
                fieldname = fieldname.substring(fieldname.indexOf('.') + 1);
        }
        return fieldname;
    }

    public String getSearchIndexName() {
        if (!queryRewritten)
            throw new IllegalStateException("Must call .rewriteQuery() before calling .getSearchIndexName()");

        return metadataManager.getMyIndex().getIndexName();
    }

    private AbstractAggregationBuilder build(ASTAggregate agg) {
        if (agg == null)
            return null;

        AbstractAggregationBuilder ab;

        if (agg instanceof ASTTally)
            ab = build((ASTTally) agg);
        else if (agg instanceof ASTRangeAggregate)
            ab = build((ASTRangeAggregate) agg);
        else if (agg instanceof ASTSignificantTerms)
            ab = build((ASTSignificantTerms) agg);
        else if (agg instanceof ASTExtendedStats)
            ab = build((ASTExtendedStats) agg);
        else
            throw new QueryRewriteException("Unrecognized aggregation type: " + agg.getClass().getName());

        ASTAggregate subagg = agg.getSubAggregate();
        if (subagg != null && ab instanceof AggregationBuilder) {
            if (!metadataManager.getMetadataForField(subagg.getFieldname()).getLink().getIndexName().equals(metadataManager.getMyIndex().getIndexName()))
                throw new QueryRewriteException("Nested aggregates in separate indexes are not supported");

            ((AggregationBuilder) ab).subAggregation(build(subagg));
        }

        if (agg.isNested()) {
            ab = nested("nested").path(agg.getNestedPath())
                    .subAggregation(
                            filter("filter")
                                    .filter(queryFilter(build(tree)))
                                    .subAggregation(ab).subAggregation(missing("missing").field(getAggregateFieldName(agg)))
                    );
        }

        return ab;
    }

    private TermSuggestionBuilder build(ASTSuggest agg) {
        if (agg == null)
            return null;

        TermSuggestionBuilder tsb = new TermSuggestionBuilder("suggestions");
        tsb.field(getSuggestFieldName());
        tsb.size(agg.getMaxTerms());
        tsb.text(agg.getStem());
        tsb.suggestMode("always");
        tsb.minWordLength(1);
        tsb.shardSize(agg.getMaxTerms() * 10);

        return tsb;
    }

    private AggregationBuilder build(ASTTally agg) {
        String fieldname = agg.getFieldname();
        IndexMetadata md = metadataManager.getMetadataForField(fieldname);
        DateHistogramIntervals interval = null;
        String intervalOffset = null;

        boolean useHistogram = false;
        if (hasDate(md, fieldname)) {
            try {
                String stem = agg.getStem();
                int colon_idx = stem.indexOf(':');

                if (colon_idx >= 0) {
                    intervalOffset = stem.substring(colon_idx + 1);
                    stem = stem.substring(0, colon_idx);
                }

                interval = DateHistogramIntervals.valueOf(stem);
                useHistogram = true;
            } catch (IllegalArgumentException iae) {
                useHistogram = false;
            }
        }

        if (useHistogram) {
            DateHistogramBuilder dhb = dateHistogram(agg.getFieldname())
                    .field(getAggregateFieldName(agg) + DateSuffix)
                    .order(stringToDateHistogramOrder(agg.getSortOrder()))
                    .offset(intervalOffset);

            switch (interval) {
                case year:
                    dhb.interval(DateHistogram.Interval.YEAR);
                    dhb.format("yyyy");
                    break;
                case month:
                    dhb.interval(DateHistogram.Interval.MONTH);
                    dhb.format("yyyy-MM");
                    break;
                case week:
                    dhb.interval(DateHistogram.Interval.WEEK);
                    dhb.format("yyyy-MM-dd");
                    break;
                case day:
                    dhb.interval(DateHistogram.Interval.DAY);
                    dhb.format("yyyy-MM-dd");
                    break;
                case hour:
                    dhb.interval(DateHistogram.Interval.HOUR);
                    dhb.format("yyyy-MM-dd HH");
                    break;
                case minute:
                    dhb.interval(DateHistogram.Interval.MINUTE);
                    dhb.format("yyyy-MM-dd HH:mm");
                    break;
                case second:
                    dhb.format("yyyy-MM-dd HH:mm:ss");
                    dhb.interval(DateHistogram.Interval.SECOND);
                    break;
                default:
                    throw new QueryRewriteException("Unsupported date histogram interval: " + agg.getStem());
            }

            return dhb;
        } else {
            TermsBuilder tb = terms(agg.getFieldname())
                    .field(getAggregateFieldName(agg))
                    .size(agg.getMaxTerms())
                    .shardSize(agg.getShardSize())
                    .order(stringToTermsOrder(agg.getSortOrder()));

            if ("string".equalsIgnoreCase(md.getType(agg.getFieldname())))
                tb.include(agg.getStem(), Pattern.CASE_INSENSITIVE);

            return tb;
        }
    }

    /**
     * Determine if a particular field name is present in the index
     *
     * @param md        index metadata
     * @param fieldname field name to check for
     * @return true if this field exists, false otherwise
     */
    private boolean hasDate(final IndexMetadata md, final String fieldname) {
        return md.hasField(fieldname + DateSuffix);
    }

    private static <T> T createRangeSpec(Class<T> type, String value) {
        try {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(value, type);
        } catch (IOException ioe) {
            throw new QueryRewriteException("Problem decoding range spec: " + value, ioe);
        }
    }

    private AggregationBuilder build(ASTRangeAggregate agg) {
        final String fieldname = agg.getFieldname();
        final IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        // if this is a date field, execute a date range aggregation
        if (hasDate(md, fieldname)) {
            final DateRangeBuilder dateRangeBuilder = new DateRangeBuilder(fieldname)
                    .field(getAggregateFieldName(agg) + DateSuffix);

            for (final DateRangeSpecEntry e : createRangeSpec(DateRangeSpecEntry[].class, agg.getRangeSpec())) {
                if (e.to == null && e.from == null)
                    throw new QueryRewriteException("Invalid range spec entry:  one of 'to' or 'from' must be specified");

                if (e.from == null)
                    dateRangeBuilder.addUnboundedTo(e.key, e.to);
                else if (e.to == null)
                    dateRangeBuilder.addUnboundedFrom(e.key, e.from);
                else
                    dateRangeBuilder.addRange(e.key, e.from, e.to);
            }

            return dateRangeBuilder;
        } else {
            // this is not a date field so execute a normal numeric range aggregation
            final RangeBuilder rangeBuilder = new RangeBuilder(fieldname)
                    .field(getAggregateFieldName(agg));

            for (final RangeSpecEntry e : createRangeSpec(RangeSpecEntry[].class, agg.getRangeSpec())) {
                if (e.to == null && e.from == null)
                    throw new QueryRewriteException("Invalid range spec entry:  one of 'to' or 'from' must be specified");

                if (e.from == null)
                    rangeBuilder.addUnboundedTo(e.key, e.to);
                else if (e.to == null)
                    rangeBuilder.addUnboundedFrom(e.key, e.from);
                else
                    rangeBuilder.addRange(e.key, e.from, e.to);
            }

            return rangeBuilder;
        }
    }

    private AggregationBuilder build(ASTSignificantTerms agg) {
        IndexMetadata md = metadataManager.getMetadataForMyIndex();
        SignificantTermsBuilder stb = significantTerms(agg.getFieldname())
                .field(getAggregateFieldName(agg))
                .size(agg.getMaxTerms());

        if ("string".equalsIgnoreCase(md.getType(agg.getFieldname())))
            stb.include(agg.getStem(), Pattern.CASE_INSENSITIVE);

        return stb;
    }

    private AbstractAggregationBuilder build(ASTExtendedStats agg) {
        return extendedStats(agg.getFieldname())
                .field(getAggregateFieldName(agg));
    }

    private static Terms.Order stringToTermsOrder(String s) {
        switch (s) {
            case "term":
                return Terms.Order.term(true);
            case "count":
                return Terms.Order.count(false);
            case "reverse_term":
                return Terms.Order.term(false);
            case "reverse_count":
                return Terms.Order.count(true);
            default:
                return null;
        }
    }

    private static DateHistogram.Order stringToDateHistogramOrder(String s) {
        switch (s) {
            case "term":
                return DateHistogram.Order.KEY_ASC;
            case "count":
                return DateHistogram.Order.COUNT_ASC;
            case "reverse_term":
                return DateHistogram.Order.KEY_DESC;
            case "reverse_count":
                return DateHistogram.Order.COUNT_DESC;
            default:
                return null;
        }
    }

    public QueryBuilder build(QueryParserNode node) {
        if (node == null)
            return null;
        else if (node instanceof ASTAnd)
            return build((ASTAnd) node);
        else if (node instanceof ASTWith)
            return build((ASTWith) node);
        else if (node instanceof ASTNot)
            return build((ASTNot) node);
        else if (node instanceof ASTOr)
            return build((ASTOr) node);
        else if (node instanceof ASTBoolQuery)
            return build((ASTBoolQuery) node);

        return build0(node);
    }

    private QueryBuilder build0(QueryParserNode node) {
        QueryBuilder qb;
        if (node instanceof ASTArray)
            qb = build((ASTArray) node);
        else if (node instanceof ASTArrayData)
            qb = build((ASTArrayData) node);
        else if (node instanceof ASTBoolean)
            qb = build((ASTBoolean) node);
        else if (node instanceof ASTFuzzy)
            qb = build((ASTFuzzy) node);
        else if (node instanceof ASTNotNull)
            qb = build((ASTNotNull) node);
        else if (node instanceof ASTNull)
            qb = build((ASTNull) node);
        else if (node instanceof ASTNumber)
            qb = build((ASTNumber) node);
        else if (node instanceof ASTPhrase)
            qb = build((ASTPhrase) node);
        else if (node instanceof ASTPrefix)
            qb = build((ASTPrefix) node);
        else if (node instanceof ASTProximity)
            qb = build((ASTProximity) node);
        else if (node instanceof ASTQueryTree)
            qb = build((ASTQueryTree) node);
        else if (node instanceof ASTRange)
            qb = build((ASTRange) node);
        else if (node instanceof ASTWildcard)
            qb = build((ASTWildcard) node);
        else if (node instanceof ASTWord)
            qb = build((ASTWord) node);
        else if (node instanceof ASTScript)
            qb = build((ASTScript) node);
        else if (node instanceof ASTExpansion)
            qb = build((ASTExpansion) node);
        else
            throw new QueryRewriteException("Unexpected node type: " + node.getClass().getName());

        maybeBoost(node, qb);

        return qb;
    }

    private void maybeBoost(QueryParserNode node, QueryBuilder qb) {
        if (qb instanceof BoostableQueryBuilder && node.getBoost() != 0.0)
            ((BoostableQueryBuilder) qb).boost(node.getBoost());
    }

    private QueryBuilder build(ASTQueryTree root) throws QueryRewriteException {
        QueryParserNode queryNode = root.getQueryNode();

        if (queryNode == null)
            return matchAllQuery();

        // and build the query
        return build(queryNode);
    }

    private QueryBuilder build(ASTAnd node) {
        BoolQueryBuilder fb = boolQuery();

        for (QueryParserNode child : node) {
            fb.must(build(child));
        }
        return fb;
    }

    private int withDepth = 0;
    private String withNestedPath;

    private QueryBuilder build(ASTWith node) {
        if (withDepth == 0)
            withNestedPath = Utils.validateSameNestedPath(node);

        BoolQueryBuilder fb = boolQuery();

        withDepth++;
        try {
            for (QueryParserNode child : node) {
                fb.must(build(child));
            }
        } finally {
            withDepth--;
        }

        if (withDepth == 0) {
            if (shouldJoinNestedFilter())
                return nestedQuery(withNestedPath, fb);
            else
                return filteredQuery(matchAllQuery(), nestedFilter(withNestedPath, fb).join(false));
        } else {
            return fb;
        }
    }

    private QueryBuilder build(ASTOr node) {
        BoolQueryBuilder fb = boolQuery();

        for (QueryParserNode child : node) {
            fb.should(build(child));
        }
        return fb;
    }

    private QueryBuilder build(ASTNot node) {
        BoolQueryBuilder qb = boolQuery();

        if (_isBuildingAggregate)
            return matchAllQuery();

        for (QueryParserNode child : node) {
            qb.mustNot(build(child));
        }
        return qb;
    }

    private String nested = null;

    protected QueryBuilder build(ASTExpansion node) {
        QueryBuilder expansionBuilder = build(node.getQuery());
        QueryParserNode filterQuery = node.getFilterQuery();
        if (filterQuery != null) {
            BoolQueryBuilder bqb = boolQuery();
            bqb.must(applyExclusion(build(node.getQuery()), node.getIndexLink().getIndexName()));
            bqb.must(build(filterQuery));
            expansionBuilder = bqb;
        }
        return expansionBuilder;
    }

    private QueryBuilder build(ASTWord node) {
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                Object value = n.getValue();

                return termQuery(n.getFieldname(), value);
            }
        });
    }

    private QueryBuilder build(ASTScript node) {
        return filteredQuery(matchAllQuery(), scriptFilter(node.getValue().toString()));
    }

    private QueryBuilder build(final ASTPhrase node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return buildStandard(node, QBF.DUMMY);

        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                MatchQueryBuilder builder = matchPhraseQuery(n.getFieldname(), n.getValue());
                if (node.getDistance() != 0)
                    builder.slop(node.getDistance());
                return builder;
            }
        });
    }

    private QueryBuilder build(ASTNumber node) {
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                Object value;

                String type = metadataManager.getMetadataForField(n.getFieldname()).getType(n.getFieldname());
                value = coerceNumber(n.getValue(), type);
                return termQuery(n.getFieldname(), value);
            }
        });
    }

    private Object coerceNumber(Object value, String type) {
        switch (type) {
            case "integer":
                value = Integer.parseInt(String.valueOf(value));
                break;
            case "long":
                value = Long.parseLong(String.valueOf(value));
                break;
            case "double":
                value = Double.parseDouble(String.valueOf(value));
                break;
            case "float":
                value = Float.parseFloat(String.valueOf(value));
                break;
            case "unknown":
                try {
                    value = Integer.valueOf(String.valueOf(value));
                } catch (Exception e) {
                    try {
                        value = Long.valueOf(String.valueOf(value));
                    } catch (Exception e2) {
                        try {
                            value = Float.valueOf(String.valueOf(value));
                        } catch (Exception e3) {
                            try {
                                value = Double.valueOf(String.valueOf(value));
                            } catch (Exception e4) {
                                // value stays unchanged
                            }
                        }
                    }
                }
                break;

            default:
                break;

        }
        return value;
    }

    private void validateOperator(QueryParserNode node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator: " + node.getOperator());
        }
    }

    private QueryBuilder build(ASTNull node) {
        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                return filteredQuery(matchAllQuery(), missingFilter(n.getFieldname()));
            }
        });
    }

    private QueryBuilder build(ASTNotNull node) {
        IndexMetadata md = metadataManager.getMetadataForField(node.getFieldname());
        if (md != null && node.getFieldname().equalsIgnoreCase(md.getPrimaryKeyFieldName())) {
            // optimization when we know every document has a value for the specified field
            switch (node.getOperator()) {
                case NE:
                    return boolQuery().mustNot(matchAllQuery());
                default:
                    return matchAllQuery();
            }
        }

        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                return filteredQuery(matchAllQuery(), existsFilter(n.getFieldname()));
            }
        });
    }

    private QueryBuilder build(ASTBoolean node) {
        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                return termQuery(n.getFieldname(), n.getValue());
            }
        });
    }

    private QueryBuilder build(final ASTFuzzy node) {
        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                return fuzzyQuery(n.getFieldname(), n.getValue()).prefixLength(n.getFuzzyness() == 0 ? 3 : n.getFuzzyness());
            }
        });
    }

    private QueryBuilder build(final ASTPrefix node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return regexpQuery(node.getFieldname(), String.valueOf(node.getValue()));

        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                return prefixQuery(n.getFieldname(), String.valueOf(n.getValue()));
            }
        });
    }

    private QueryBuilder build(final ASTWildcard node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return regexpQuery(node.getFieldname(), String.valueOf(node.getValue()));

        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                return wildcardQuery(n.getFieldname(), String.valueOf(n.getValue()));
            }
        });
    }

    private QueryBuilder build(final ASTArray node) {
        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(final QueryParserNode n) {
                boolean isNE = node.getOperator() == QueryParserNode.Operator.NE;
                Iterable<Object> itr = node.hasExternalValues() ? node.getExternalValues() : node.getChildValues();
                final int cnt = node.hasExternalValues() ? node.getTotalExternalValues() : node.jjtGetNumChildren();
                int minShouldMatch = (node.isAnd() && !isNE) || (!node.isAnd() && isNE) ? cnt : 1;
                final String type = metadataManager.getMetadataForField(n.getFieldname()).getType(n.getFieldname());

                switch (type) {
                    case "integer":
                    case "long":
                    case "double":
                    case "float":
                    case "unknown": {
                        final Iterable<Object> finalItr = itr;
                        itr = new Iterable<Object>() {
                            @Override
                            public Iterator<Object> iterator() {
                                final Iterator<Object> iterator = finalItr.iterator();
                                return new Iterator<Object>() {
                                    @Override
                                    public boolean hasNext() {
                                        return iterator.hasNext();
                                    }

                                    @Override
                                    public Object next() {
                                        Object value = iterator.next();
                                        return coerceNumber(value, type);
                                    }

                                    @Override
                                    public void remove() {
                                        iterator.remove();
                                    }
                                };
                            }
                        };
                    }
                }
                if (node.hasExternalValues() && minShouldMatch == 1 && node.getTotalExternalValues() >= 1024) {
                    TermsFilterBuilder builder = termsFilter(n.getFieldname(), itr).cache(true);
                    return filteredQuery(matchAllQuery(), builder);
                } else {
                    final Iterable<Object> finalItr = itr;
                    TermsQueryBuilder builder = termsQuery(n.getFieldname(), new AbstractCollection<Object>() {
                        @Override
                        public Iterator<Object> iterator() {
                            return finalItr.iterator();
                        }

                        @Override
                        public int size() {
                            return cnt;
                        }
                    });

                    if (minShouldMatch > 1)
                        builder.minimumMatch(minShouldMatch);
                    return builder;
                }
            }
        });
    }

    private QueryBuilder build(final ASTArrayData node) {
        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                if ("_id".equals(node.getFieldname())) {
                    final EscapingStringTokenizer st = new EscapingStringTokenizer(arrayData.get(node.getValue().toString()), ", \r\n\t\f\"'[]");
                    Collection<String> terms = st.getAllTokens();
                    return idsQuery().addIds(terms.toArray(new String[terms.size()]));
                } else {
                    String data = arrayData.get(node.getValue().toString());
                    List values;

                    if (data.length() > 0 && "_zdb_id".equals(node.getFieldname())) {
                        values = Arrays.asList(data.split(","));
                    } else {
                        final EscapingStringTokenizer st = new EscapingStringTokenizer(data, ", \r\n\t\f\"'[]");
                        values = st.getAllTokens();
                    }

                    if (values.size() >= 8000) {
                        int STEP = 8000;
                        String key = "";
                        OrFilterBuilder ofb = orFilter();
                        
                        for (int i = 0, size = values.size(); i < size; i += STEP) {
                            List sublist = values.subList(i, Math.min(size, i + STEP));
                            String id = UUID.nameUUIDFromBytes(sublist.toString().getBytes()).toString();
                            ofb.add(termsFilter(n.getFieldname(), sublist).cache(true).cacheKey(id));
                            key += id;
                        }
                        return filteredQuery(null, ofb.cache(true).cacheKey(key));

                    } else {
                        if (values.size() == 1) {
                            return termQuery(node.getFieldname(), values.get(0));
                        } else {
                            return filteredQuery(matchAllQuery(), termsFilter(n.getFieldname(), values).cache(true));
                        }
                    }
                }
            }
        });
    }

    private QueryBuilder build(final ASTRange node) {
        validateOperator(node);
        return buildStandard(node, new QBF() {
            @Override
            public QueryBuilder b(QueryParserNode n) {
                QueryParserNode start = n.getChild(0);
                QueryParserNode end = n.getChild(1);
                return rangeQuery(node.getFieldname()).from(coerceNumber(start.getValue(), "unknown")).to(coerceNumber(end.getValue(), "unknown"));
            }
        });
    }

    private QueryBuilder build(ASTBoolQuery node) {
        BoolQueryBuilder builder = boolQuery();
        ASTMust must = node.getMust();
        ASTShould should = node.getShould();
        ASTMustNot mustNot = node.getMustNot();

        if (must != null)
            for (QueryParserNode child : must)
                builder.must(build(child));

        if (should != null)
            for (QueryParserNode child : should)
                builder.should(build(child));

        if (mustNot != null)
            for (QueryParserNode child : mustNot)
                builder.mustNot(build(child));

        return builder;
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, QueryParserNode node) {
        SpanQueryBuilder qb;

        if (node instanceof ASTWord)
            qb = buildSpan(prox, (ASTWord) node);
        else if (node instanceof ASTNumber)
            qb = buildSpan(prox, (ASTNumber) node);
        else if (node instanceof ASTBoolean)
            qb = buildSpan(prox, (ASTBoolean) node);
        else if (node instanceof ASTFuzzy)
            qb = buildSpan(prox, (ASTFuzzy) node);
        else if (node instanceof ASTPrefix)
            qb = buildSpan(prox, (ASTPrefix) node);
        else if (node instanceof ASTWildcard)
            qb = buildSpan(prox, (ASTWildcard) node);
        else if (node instanceof ASTPhrase)
            qb = buildSpan(prox, (ASTPhrase) node);
        else if (node instanceof ASTNull)
            qb = buildSpan(prox, (ASTNull) node);
        else if (node instanceof ASTNotNull)
            return buildSpan(prox, (ASTNotNull) node);
        else if (node instanceof ASTOr)
            qb = buildSpan(prox, (ASTOr) node);
        else if (node instanceof ASTProximity)
            qb = buildSpan((ASTProximity) node);
        else
            throw new QueryRewriteException("Unsupported PROXIMITY node: " + node.getClass().getName());

        maybeBoost(node, qb);
        return qb;
    }

    private SpanQueryBuilder buildSpan(ASTProximity node) {
        SpanNearQueryBuilder qb = spanNearQuery();

        for (QueryParserNode child : node) {
            qb.clause(buildSpan(node, child));
        }

        qb.slop(node.getDistance());
        qb.inOrder(node.isOrdered());
        return qb;
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTWord node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        return spanTermQuery(node.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNull node) {
        // when building spans, treat 'null' as a regular term
        return spanTermQuery(node.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNotNull node) {
        return spanMultiTermQueryBuilder(wildcardQuery(node.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNumber node) {
        return spanTermQuery(node.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTBoolean node) {
        return spanTermQuery(node.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTFuzzy node) {
        return spanMultiTermQueryBuilder(fuzzyQuery(node.getFieldname(), node.getValue()).prefixLength(node.getFuzzyness() == 0 ? 3 : node.getFuzzyness()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTPrefix node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        return spanMultiTermQueryBuilder(prefixQuery(node.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTWildcard node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        return spanMultiTermQueryBuilder(wildcardQuery(node.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTPhrase node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        return buildSpan(prox, Utils.convertToProximity(node.getFieldname(), Utils.analyzeForSearch(client, metadataManager, node.getFieldname(), node.getEscapedValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTOr node) {
        SpanOrQueryBuilder or = spanOrQuery();
        for (QueryParserNode child : node)
            or.clause(buildSpan(prox, child));
        return or;
    }

    private QueryBuilder build(ASTProximity node) {
        if (node.getFieldname() != null)
            node.forceFieldname(node.getFieldname());

        SpanNearQueryBuilder qb = spanNearQuery();
        qb.slop(node.getDistance());
        qb.inOrder(node.isOrdered());

        for (QueryParserNode child : node) {
            qb.clause(buildSpan(node, child));
        }

        return qb;
    }

    private QueryBuilder buildStandard(QueryParserNode node, QBF qbf) {
        return maybeNest(node, buildStandard0(node, qbf));
    }

    private QueryBuilder buildStandard0(QueryParserNode node, QBF qbf) {
        switch (node.getOperator()) {
            case EQ:
            case CONTAINS:
                return qbf.b(node);

            case NE:
                return boolQuery().mustNot(qbf.b(node));
            case LT:
                return rangeQuery(node.getFieldname()).lt(coerceNumber(node.getValue(), "unknown"));
            case GT:
                return rangeQuery(node.getFieldname()).gt(coerceNumber(node.getValue(), "unknown"));
            case LTE:
                return rangeQuery(node.getFieldname()).lte(coerceNumber(node.getValue(), "unknown"));
            case GTE:
                return rangeQuery(node.getFieldname()).gte(coerceNumber(node.getValue(), "unknown"));

            case REGEX:
                return regexpQuery(node.getFieldname(), node.getEscapedValue());

            case CONCEPT: {
                int minTermFreq = 2;
                // drop the minTermFreq to 1 if we
                // determine that the field being queried is NOT of type "fulltext"
                IndexMetadata md = metadataManager.getMetadataForField(node.getFieldname());
                if (md != null)
                    if (!"fulltext".equalsIgnoreCase(md.getSearchAnalyzer(node.getFieldname())))
                        minTermFreq = 1;

                return moreLikeThisQuery(node.getFieldname()).likeText(String.valueOf(node.getValue())).maxQueryTerms(80).minWordLength(3).minTermFreq(minTermFreq).stopWords(IndexMetadata.MLT_STOP_WORDS);
            }

            case FUZZY_CONCEPT:
                return fuzzyLikeThisFieldQuery(node.getFieldname()).likeText(String.valueOf(node.getValue())).maxQueryTerms(80).fuzziness(Fuzziness.AUTO);

            default:
                throw new QueryRewriteException("Unexpected operator: " + node.getOperator());
        }
    }

    private QueryBuilder maybeNest(QueryParserNode node, QueryBuilder fb) {
        if (withDepth == 0 && node.isNested(metadataManager)) {
            if (shouldJoinNestedFilter())
                return nestedQuery(node.getNestedPath(), fb);
            else
                return filteredQuery(matchAllQuery(), nestedFilter(node.getNestedPath(), fb).join(false));
        } else if (!node.isNested(metadataManager)) {
            if (_isBuildingAggregate)
                return matchAllQuery();
            return fb;  // it's not nested, so just return
        }


        if (nested != null) {
            // we are currently nesting, so make sure this node's path
            // matches the one we're in
            if (node.getNestedPath().equals(nested))
                return fb;  // since we're already nesting, no need to do anything
            else
                throw new QueryRewriteException("Attempt to use nested path '" + node.getNestedPath() + "' inside '" + nested + "'");
        }

        return fb;
    }


    private boolean shouldJoinNestedFilter() {
        return !_isBuildingAggregate || !tree.getAggregate().isNested();
    }

    public QueryBuilder applyExclusion(QueryBuilder query, String indexName) {
        QueryParserNode exclusion = tree.getExclusion(indexName);

        if (exclusion != null) {
            BoolQueryBuilder bqb = boolQuery();
            bqb.must(query);
            if (exclusion instanceof ASTOr) {
                for (QueryParserNode child : exclusion)
                    bqb.mustNot(build(child));
            } else {
                bqb.mustNot(build(exclusion));
            }
            query = bqb;
        }
        return query;
    }
}
