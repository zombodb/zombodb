/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2019 ZomboDB, LLC
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
package llc.zombodb.query_parser.rewriters;

import llc.zombodb.query_parser.*;
import llc.zombodb.query_parser.metadata.IndexMetadata;
import llc.zombodb.query_parser.metadata.IndexMetadataManager;
import llc.zombodb.query_parser.optimizers.ArrayDataOptimizer;
import llc.zombodb.query_parser.optimizers.IndexLinkOptimizer;
import llc.zombodb.query_parser.optimizers.TermAnalyzerOptimizer;
import llc.zombodb.query_parser.utils.EscapingStringTokenizer;
import llc.zombodb.query_parser.utils.Utils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import static llc.zombodb.visibility_query.ZomboDBQueryBuilders.visibility;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

public abstract class QueryRewriter {

    public static class Factory {
        public static QueryRewriter create(RestRequest restRequest, Client client, String indexName, String input, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel, boolean wantScores) {
            return new ZomboDBQueryRewriter(client, indexName, restRequest.getXContentRegistry(), input, canDoSingleIndex, needVisibilityOnTopLevel, wantScores);
        }
    }

    private enum DateHistogramIntervals {
        year, quarter, month, week, day, hour, minute, second
    }

    /* short for QueryBuilderFactory */
    private interface QBF {
        QBF DUMMY = n -> {
            throw new QueryRewriteException("Should not get here");
        };

        QueryBuilder b(QueryParserNode n);
    }

    /**
     * Container for range aggregation spec
     * <p>
     * The class and its members *must* be public so that
     * ObjectMapper can instantiate them
     */
    public static class RangeSpecEntry {
        public String key;
        public Double from;
        public Double to;
    }

    /**
     * Container for date range aggregation spec
     * <p>
     * The class and its members *must* be public so that
     * ObjectMapper can instantiate them
     */
    public static class DateRangeSpecEntry {
        public String key;
        public String from;
        public String to;
    }

    static class QueryRewriteException extends RuntimeException {
        QueryRewriteException(String message) {
            super(message);
        }

        QueryRewriteException(Throwable cause) {
            super(cause);
        }

        public QueryRewriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final String DateSuffix = ".date";

    protected final Client client;
    private final NamedXContentRegistry contentRegistry;
    private boolean needVisibilityOnTopLevel;
    private final ASTQueryTree tree;

    boolean _isBuildingAggregate = false;
    private boolean queryRewritten = false;

    private Map<String, String> arrayData;

    final IndexMetadataManager metadataManager;
    private boolean hasJsonAggregate = false;
    private final boolean wantScores;

    public QueryRewriter(Client client, String indexName, NamedXContentRegistry contentRegistry, String input, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel, boolean wantScores) {
        this.client = client;
        this.contentRegistry = contentRegistry;
        this.needVisibilityOnTopLevel = needVisibilityOnTopLevel;

        metadataManager = new IndexMetadataManager(client, indexName);

        final StringBuilder newQuery = new StringBuilder(input.length());
        final Set<String> usedFields;
        try {
            arrayData = Utils.extractArrayData(input, newQuery);

            llc.zombodb.query_parser.QueryParser parser = new llc.zombodb.query_parser.QueryParser(new StringReader(newQuery.toString()));
            tree = parser.parse(metadataManager, true);
            usedFields = parser.getUsedFieldnames();
            if (tree.getLimit() != null)
                this.needVisibilityOnTopLevel = true;
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

        Set<ASTIndexLink> usedLinks = new HashSet<>();
        for (String field : usedFields) {
            usedLinks.add(metadataManager.findField(field));
        }

        if (!metadataManager.getMetadataForMyIndex().alwaysResolveJoins()) {
            if (!hasJsonAggregate && canDoSingleIndex && !hasAgg && usedLinks.size() == 1) {
                metadataManager.setMyIndex(usedLinks.iterator().next());
            }
        }

        metadataManager.loadExpansionMappings(tree);
        performOptimizations(client);

        this.wantScores = wantScores || (getLimit() != null && "_score".equals(getLimit().getFieldname()));
    }

    /**
     * Subclasses can override if additional optimizations are necessary, but
     * they should definitely call {@link super.performOptimizations(Client)}
     */
    private void performOptimizations(Client client) {
        new ArrayDataOptimizer(tree, metadataManager, arrayData).optimize();
        new IndexLinkOptimizer(client, this, tree, metadataManager).optimize();
        new TermAnalyzerOptimizer(client, metadataManager, tree).optimize();
    }

    public String dumpAsString() {
        return tree.dumpAsString();
    }

    public Map<String, ?> describedNestedObject(String fieldname) {
        return metadataManager.describedNestedObject(fieldname);
    }

    public ASTLimit getLimit() {
        return tree.getLimit();
    }

    public boolean wantScores() {
        return wantScores;
    }

    public QueryBuilder rewriteQuery() {
        QueryBuilder qb = build(tree);
        queryRewritten = true;

        if (qb == null)
            qb = new MatchNoneQueryBuilder();

        return needVisibilityOnTopLevel ? applyVisibility(qb) : qb;
    }

    public AggregationBuilder rewriteAggregations() {
        try {
            _isBuildingAggregate = true;
            return build(tree.getAggregate());
        } finally {
            _isBuildingAggregate = false;
        }
    }

    public boolean isAggregateNested() {
        return tree.getAggregate().isNested(metadataManager.getMetadataForField(tree.getAggregate().getFieldname()));
    }

    public boolean hasJsonAggregate() {
        return hasJsonAggregate;
    }

    public SuggestionBuilder rewriteSuggestions() {
        try {
            _isBuildingAggregate = true;
            return build(tree.getSuggest());
        } finally {
            _isBuildingAggregate = false;
        }
    }

    public String getAggregateIndexName() {
        ASTIndexLink link;
        if (tree.getAggregate() != null)
            link = metadataManager.findField(tree.getAggregate().getFieldname());
        else if (tree.getSuggest() != null)
            link = metadataManager.findField(tree.getSuggest().getFieldname());
        else
            throw new QueryRewriteException("Cannot figure out which index to use for aggregation");

        return link.getAlias() != null ? link.getAlias() : link.getIndexName();
    }

    public String getAggregateFieldName() {
        return getAggregateFieldName(tree.getAggregate());

    }

    private String getAggregateFieldName(ASTAggregate agg) {
        String fieldname = agg.getFieldname();
        IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        fieldname = maybeStripFieldname(fieldname, md);

        if (hasDate(md, fieldname))
            fieldname += DateSuffix;

        return fieldname;
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

        return metadataManager.getMyIndex().getAlias() == null ? metadataManager.getMyIndex().getIndexName() : metadataManager.getMyIndex().getAlias();
    }

    private AggregationBuilder build(ASTAggregate agg) {
        if (agg == null)
            return null;

        AggregationBuilder ab;

        if (agg instanceof ASTTally)
            ab = build((ASTTally) agg);
        else if (agg instanceof ASTJsonAgg)
            ab = build((ASTJsonAgg) agg);
        else if (agg instanceof ASTRangeAggregate)
            ab = build((ASTRangeAggregate) agg);
        else if (agg instanceof ASTSignificantTerms)
            ab = build((ASTSignificantTerms) agg);
        else if (agg instanceof ASTExtendedStats)
            ab = build((ASTExtendedStats) agg);
        else
            throw new QueryRewriteException("Unrecognized aggregation type: " + agg.getClass().getName());

        ASTAggregate subagg = agg.getSubAggregate();
        if (subagg != null) {
            if (!metadataManager.getMetadataForField(subagg.getFieldname()).getLink().getIndexName().equals(metadataManager.getMyIndex().getIndexName()))
                throw new QueryRewriteException("Nested aggregates in separate indexes are not supported");

            ab.subAggregation(build(subagg));
        }

        if (metadataManager.getMetadataForField(agg.getFieldname()).isNested(agg.getFieldname())) {
            if (agg.isSpecifiedAsNested()) {
                ab = nested("nested", agg.getNestedPath(metadataManager))
                        .subAggregation(
                                filter("filter", build(tree))
                                        .subAggregation(ab).subAggregation(missing("missing").field(getAggregateFieldName(agg)))
                        );
            } else {
                ab = nested("nested", agg.getNestedPath(metadataManager))
                        .subAggregation(ab).subAggregation(missing("missing").field(getAggregateFieldName(agg)));
            }
        }

        return ab;
    }

    private TermSuggestionBuilder build(ASTSuggest agg) {
        if (agg == null)
            return null;

        TermSuggestionBuilder tsb = new TermSuggestionBuilder(agg.getFieldname());
        tsb.size(agg.getMaxTerms());
        tsb.text(agg.getStem());
        tsb.suggestMode(TermSuggestionBuilder.SuggestMode.ALWAYS);
        tsb.minWordLength(1);
        tsb.shardSize(Integer.MAX_VALUE);

        return tsb;
    }

    private AggregationBuilder build(ASTTally agg) {
        String fieldname = agg.getFieldname();
        IndexMetadata md = metadataManager.getMetadataForField(fieldname);
        DateHistogramIntervals interval = null;
        String intervalOffset = null;
        boolean isdate = hasDate(md, fieldname);

        fieldname = maybeStripFieldname(fieldname, md);
        boolean useHistogram = false;
        if (isdate) {
                String stem = agg.getStem();
                int colon_idx = stem.indexOf(':');

                if (colon_idx >= 0) {
                    intervalOffset = stem.substring(colon_idx + 1);
                    stem = stem.substring(0, colon_idx);
                }

            try {
                interval = DateHistogramIntervals.valueOf(stem);
                useHistogram = true;
                fieldname += DateSuffix;
            } catch (IllegalArgumentException iae) {
                // caller wants to use a regex stem instead of
                // the histogram (because it didn't parse correctly)
                // so lets not treat it as a date
                useHistogram = false;
            }
        }

        if (useHistogram) {
            DateHistogramAggregationBuilder dhb = dateHistogram(agg.getFieldname())
                    .field(fieldname)
                    .order(stringToDateHistogramOrder(agg.getSortOrder()))
                    .minDocCount(1);

            if (intervalOffset != null)
                    dhb.offset(intervalOffset);

            switch (interval) {
                case year:
                    dhb.dateHistogramInterval(DateHistogramInterval.YEAR);
                    dhb.format("yyyy");
                    break;
                case month:
                    dhb.dateHistogramInterval(DateHistogramInterval.MONTH);
                    dhb.format("yyyy-MM");
                    break;
                case week:
                    dhb.dateHistogramInterval(DateHistogramInterval.WEEK);
                    dhb.format("yyyy-MM-dd");
                    break;
                case day:
                    dhb.dateHistogramInterval(DateHistogramInterval.DAY);
                    dhb.format("yyyy-MM-dd");
                    break;
                case hour:
                    dhb.dateHistogramInterval(DateHistogramInterval.HOUR);
                    dhb.format("yyyy-MM-dd HH");
                    break;
                case minute:
                    dhb.dateHistogramInterval(DateHistogramInterval.MINUTE);
                    dhb.format("yyyy-MM-dd HH:mm");
                    break;
                case second:
                    dhb.dateHistogramInterval(DateHistogramInterval.SECOND);
                    dhb.format("yyyy-MM-dd HH:mm:ss");
                    break;
                default:
                    throw new QueryRewriteException("Unsupported date histogram interval: " + agg.getStem());
            }

            return dhb;
        } else {
            TermsAggregationBuilder tb = terms(agg.getFieldname())
                    .field(fieldname)
                    .size(agg.getMaxTerms() == 0 ? Integer.MAX_VALUE : agg.getMaxTerms())
                    .shardSize(agg.getShardSize() == 0 ? Integer.MAX_VALUE : agg.getShardSize())
                    .order(stringToTermsOrder(agg.getSortOrder()));

            if ("string".equalsIgnoreCase(md.getType(agg.getFieldname())))
                tb.includeExclude(new IncludeExclude(agg.getStem(), null));

            return tb;
        }
    }

    private AggregationBuilder build(final ASTJsonAgg node) {
        hasJsonAggregate = true;
        try {
            QueryParseContext context = new QueryParseContext(JsonXContent.jsonXContent.createParser(contentRegistry, node.getEscapedValue()));

            // consume the OBJECT_START token
            context.parser().nextToken();

            // and then let ES finish parsing the aggregate for us
            AggregatorFactories.Builder builder = AggregatorFactories.parseAggregators(context);
            return builder.getAggregatorFactories().get(0);
        } catch (IOException e) {
           throw new RuntimeException(e);
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

    private AggregationBuilder build(ASTRangeAggregate agg) {
        final String fieldname = agg.getFieldname();
        final IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        // if this is a date field, execute a date range aggregation
        if (hasDate(md, fieldname)) {
            final DateRangeAggregationBuilder dateRangeBuilder = dateRange(fieldname)
                    .field(getAggregateFieldName(agg));

            for (final DateRangeSpecEntry e : Utils.jsonToObject(agg.getRangeSpec(), DateRangeSpecEntry[].class)) {
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
            final RangeAggregationBuilder rangeBuilder = range(fieldname)
                    .field(getAggregateFieldName(agg));

            for (final RangeSpecEntry e : Utils.jsonToObject(agg.getRangeSpec(), RangeSpecEntry[].class)) {
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
        SignificantTermsAggregationBuilder stb = significantTerms(agg.getFieldname())
                .field(getAggregateFieldName(agg))
                .size(agg.getMaxTerms());

        if ("string".equalsIgnoreCase(md.getType(agg.getFieldname())))
            stb.includeExclude(new IncludeExclude(agg.getStem(), null));

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

    private static Histogram.Order stringToDateHistogramOrder(String s) {
        switch (s) {
            case "term":
                return Histogram.Order.KEY_ASC;
            case "count":
                return Histogram.Order.COUNT_ASC;
            case "reverse_term":
                return Histogram.Order.KEY_DESC;
            case "reverse_count":
                return Histogram.Order.COUNT_DESC;
            default:
                return null;
        }
    }

    public QueryBuilder build(QueryParserNode node) {
        if (node == null)
            return new MatchNoneQueryBuilder();
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
        else if (node instanceof ASTJsonQuery)
            qb = build((ASTJsonQuery) node);
        else
            throw new QueryRewriteException("Unexpected node type: " + node.getClass().getName());

        maybeBoost(node, qb);

        return qb;
    }

    private void maybeBoost(QueryParserNode node, QueryBuilder qb) {
        if (node.getBoost() != 0.0)
            qb.boost(node.getBoost());
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

        int cnt = 0;
        QueryBuilder last = null;
        for (QueryParserNode child : node) {
            QueryBuilder qb = build(child);

            if (qb instanceof MatchAllQueryBuilder)
                continue;

            fb.must(last = qb);
            cnt++;
        }

        if (cnt == 1)
            return last;

        return fb;
    }

    private boolean inWith = false;
    private QueryBuilder build(ASTWith node) {
        BoolQueryBuilder bqb = boolQuery();
        String path = null;

        inWith = true;
        try {
            for (QueryParserNode child : node) {
                if (path == null)
                    path = child.getNestedPath(metadataManager);
                bqb.must(build(child));
            }
        } finally {
            inWith = false;
        }

        return shouldJoinNestedFilter() ? nestedQuery(path, bqb, ScoreMode.Avg) : bqb;
    }

    private QueryBuilder build(ASTOr node) {
        BoolQueryBuilder fb = boolQuery();

        int cnt = 0;
        boolean hasMatchAll = false;
        QueryBuilder last = null;
        for (QueryParserNode child : node) {
            QueryBuilder qb = build(child);

            if (qb instanceof MatchAllQueryBuilder && hasMatchAll)
                continue;

            fb.should(last = qb);

            if (qb instanceof MatchAllQueryBuilder)
                hasMatchAll = true;
            cnt++;
        }

        if (cnt == 1)
            return last;

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

    QueryBuilder build(ASTExpansion node) {
        QueryBuilder expansionBuilder = build(node.getQuery());
        QueryParserNode filterQuery = node.getFilterQuery();
        if (filterQuery != null) {
            BoolQueryBuilder bqb = boolQuery();
            bqb.must(expansionBuilder);
            bqb.must(build(filterQuery));
            expansionBuilder = applyVisibility(bqb);
        }
        return expansionBuilder;
    }

    private QueryBuilder build(final ASTJsonQuery node) {
        try {
            QueryParseContext context = new QueryParseContext(JsonXContent.jsonXContent.createParser(contentRegistry, node.getEscapedValue()));
            return context.parseInnerQueryBuilder().orElse(null);
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    private QueryBuilder build(ASTWord node) {
        return buildStandard(node, n -> {
            Object value = n.getValue();

            return termQuery(n.getFieldname(), value);
        });
    }

    private QueryBuilder build(ASTScript node) {
        return scriptQuery(new Script(node.getValue().toString()));
    }

    private QueryBuilder build(final ASTPhrase node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return buildStandard(node, QBF.DUMMY);

        return buildStandard(node, n -> {
            MatchPhraseQueryBuilder builder = matchPhraseQuery(n.getFieldname(), n.getValue());
            if (node.getDistance() != 0)
                builder.slop(node.getDistance());
            return builder;
        });
    }

    private QueryBuilder build(ASTNumber node) {
        return buildStandard(node, n -> {
            Object value;

            String type = metadataManager.getMetadataForField(n.getFieldname()).getType(n.getFieldname());
            value = coerceNumber(n.getValue(), type);
            return termQuery(n.getFieldname(), value);
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
                            // we'll just stop at double.  might as well
                            // get the most precision we can out of the value
                            value = Double.valueOf(String.valueOf(value));
                        } catch (Exception e3) {
                            // value stays unchanged
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
        String fieldname = node.getFieldname();

        if (metadataManager.getMetadataForField(fieldname).isNestedObjectField(fieldname)) {
            // for queries like:  WHERE ... ==> 'json_field = NULL'
            switch (node.getOperator()) {
                case EQ:
                case CONTAINS:
                    return boolQuery().mustNot(nestedQuery(fieldname, boolQuery().mustNot(existsQuery(fieldname + ".zdb_always_exists")), ScoreMode.None));
                case NE:
                    return nestedQuery(fieldname, boolQuery().mustNot(existsQuery(fieldname + ".zdb_always_exists")), ScoreMode.None);
                default:
                    throw new QueryRewriteException("Unsupported operator for ASTNull: " + node.getOperator());
            }
        } else {
            return buildStandard(node, n -> boolQuery().mustNot(existsQuery(n.getFieldname())));
        }
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
        return buildStandard(node, n -> existsQuery(n.getFieldname()));
    }

    private QueryBuilder build(ASTBoolean node) {
        validateOperator(node);
        return buildStandard(node, n -> termQuery(n.getFieldname(), n.getValue()));
    }

    private QueryBuilder build(final ASTFuzzy node) {
        validateOperator(node);
        return buildStandard(node, n -> fuzzyQuery(n.getFieldname(), n.getValue()).prefixLength(n.getFuzzyness() == 0 ? 3 : n.getFuzzyness()));
    }

    private QueryBuilder build(final ASTPrefix node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return regexpQuery(node.getFieldname(), String.valueOf(node.getValue()));

        validateOperator(node);
        return buildStandard(node, n -> prefixQuery(n.getFieldname(), String.valueOf(n.getValue())));
    }

    private QueryBuilder build(final ASTWildcard node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return regexpQuery(node.getFieldname(), String.valueOf(node.getValue()));

        validateOperator(node);
        return buildStandard(node, n -> wildcardQuery(n.getFieldname(), String.valueOf(n.getValue())));
    }

    private QueryBuilder build(final ASTArray node) {
        validateOperator(node);
        return buildStandard(node, n -> {
            Iterable<Object> itr = node.hasExternalValues() ? node.getExternalValues() : node.getChildValues();
            final int cnt = node.hasExternalValues() ? node.getTotalExternalValues() : node.jjtGetNumChildren();
            int minShouldMatch = node.isAnd() ? cnt : 1;
            final String type = metadataManager.getMetadataForField(n.getFieldname()).getType(n.getFieldname());
            boolean isNumber = false;

            switch (type) {
                case "integer":
                case "long":
                case "double":
                case "float":
                    isNumber = true;
                    // fall-through
                case "unknown": {
                    final Iterable<Object> finalItr = itr;
                    itr = () -> {
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
                    };
                }
            }

            if ((isNumber && minShouldMatch == 1) || (node.hasExternalValues() && minShouldMatch == 1 && node.getTotalExternalValues() >= 1024)) {
                final Iterable<Object> finalItr1 = itr;
                TermsQueryBuilder builder = termsQuery(n.getFieldname(), new AbstractCollection<Object>() {
                    @Override
                    public Iterator<Object> iterator() {
                        return finalItr1.iterator();
                    }

                    @Override
                    public int size() {
                        return cnt;
                    }
                });
                return constantScoreQuery(builder);
            } else {
                if (minShouldMatch > 1) {
                    BoolQueryBuilder bool = boolQuery();
                    for (Object o : itr)
                        bool.must(termQuery(n.getFieldname(), o));
                    return bool;
                } else {
                    final Iterable<Object> finalItr = itr;

                    return termsQuery(n.getFieldname(), new AbstractCollection<Object>() {
                        @Override
                        public Iterator<Object> iterator() {
                            return finalItr.iterator();
                        }

                        @Override
                        public int size() {
                            return cnt;
                        }
                    });
                }
            }
        });
    }

    private QueryBuilder build(final ASTArrayData node) {
        validateOperator(node);
        return buildStandard(node, n -> {
            if ("_id".equals(node.getFieldname())) {
                final EscapingStringTokenizer st = new EscapingStringTokenizer(arrayData.get(node.getValue().toString()), ", \r\n\t\f\"'[]");
                Collection<String> terms = st.getAllTokens();
                return idsQuery().addIds(terms.toArray(new String[terms.size()]));
            } else {
                final EscapingStringTokenizer st = new EscapingStringTokenizer(arrayData.get(node.getValue().toString()), ", \r\n\t\f\"'[]");
                final List<String> tokens = st.getAllTokens();
                final String type = metadataManager.getMetadataForField(n.getFieldname()).getType(n.getFieldname());
                return constantScoreQuery(termsQuery(node.getFieldname(), new AbstractCollection<Object>() {
                    @Override
                    public Iterator<Object> iterator() {
                        final Iterator<String> itr = tokens.iterator();
                        return new Iterator<Object>() {
                            @Override
                            public boolean hasNext() {
                                return itr.hasNext();
                            }

                            @Override
                            public Object next() {
                                String value = itr.next();
                                try {
                                    return coerceNumber(value, type);
                                } catch (Exception e) {
                                    return value;
                                }
                            }

                            @Override
                            public void remove() {
                                itr.remove();
                            }
                        };
                    }

                    @Override
                    public int size() {
                        return tokens.size();
                    }
                }));
            }
        });
    }

    private QueryBuilder build(final ASTRange node) {
        validateOperator(node);
        return buildStandard(node, n -> {
            QueryParserNode start = n.getChild(0);
            QueryParserNode end = n.getChild(1);
            return rangeQuery(node.getFieldname()).from(coerceNumber(start.getValue(), "unknown")).to(coerceNumber(end.getValue(), "unknown"));
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
        SpanNearQueryBuilder qb = spanNearQuery(buildSpan(node, node.getChild(0)), node.getDistance());

        for (int i=1; i<node.jjtGetNumChildren(); i++) {
            qb.addClause(buildSpan(node, node.getChild(i)));
        }

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
        SpanOrQueryBuilder or = spanOrQuery(buildSpan(prox, node.getChild(0)));
        for (int i=1; i<node.jjtGetNumChildren(); i++)
            or.addClause(buildSpan(prox, node.getChild(i)));
        return or;
    }

    private QueryBuilder build(ASTProximity node) {
        if (node.getFieldname() != null)
            node.forceFieldname(node.getFieldname());

        SpanNearQueryBuilder qb = spanNearQuery(buildSpan(node, node.getChild(0)), node.getDistance());
        qb.inOrder(node.isOrdered());

        for (int i=1; i<node.jjtGetNumChildren(); i++) {
            qb.addClause(buildSpan(node, node.getChild(i)));
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

                return moreLikeThisQuery(new String[] { node.getFieldname() }, new String[] { String.valueOf(node.getValue()) }, null).maxQueryTerms(80).minWordLength(3).minTermFreq(minTermFreq).stopWords(IndexMetadata.MLT_STOP_WORDS);
            }

            case FUZZY_CONCEPT:
                return fuzzyQuery(node.getFieldname(), node.getValue()).maxExpansions(80).fuzziness(Fuzziness.AUTO);

            default:
                throw new QueryRewriteException("Unexpected operator: " + node.getOperator());
        }
    }

    private QueryBuilder maybeNest(QueryParserNode node, QueryBuilder fb) {
        if (!(node.jjtGetParent() instanceof ASTWith) && node.isNested(metadataManager)) {
            if (!inWith && shouldJoinNestedFilter())
                return nestedQuery(node.getNestedPath(metadataManager), fb, ScoreMode.Avg);
            else
                return fb;
        } else {
            if (!node.isNested(metadataManager)) {
                if (_isBuildingAggregate)
                    return matchAllQuery();
                return fb;  // it's not nested, so just return
            } else {
                if (_isBuildingAggregate) {
                    if (tree.getAggregate().getNestedPath(metadataManager).equals(node.getNestedPath(metadataManager)))
                        return fb;  // it's the same nested object as the aggregate itself
                    else
                        return matchAllQuery(); // it's a different nested object, so we can just ignore it
                }

                return fb;
            }
        }
    }


    private boolean shouldJoinNestedFilter() {
        return !_isBuildingAggregate || !tree.getAggregate().isNested(metadataManager.getMetadataForField(tree.getAggregate().getFieldname()));
    }

    public QueryBuilder getVisibilityFilter() {
        ASTVisibility visibility = tree.getVisibility();

        if (visibility == null)
            return matchAllQuery();

        return
                boolQuery()
                        .mustNot(
                                visibility()
                                        .myXid(visibility.getMyXid())
                                        .xmin(visibility.getXmin())
                                        .xmax(visibility.getXmax())
                                        .commandId(visibility.getCommandId())
                                        .activeXids(visibility.getActiveXids())
                        );
    }

    QueryBuilder applyVisibility(QueryBuilder query) {
        ASTVisibility visibility = tree.getVisibility();

        if (visibility == null)
            return query;

        return wantScores ?
                boolQuery()
                        .must(query)
                        .mustNot(
                                visibility()
                                        .myXid(visibility.getMyXid())
                                        .xmin(visibility.getXmin())
                                        .xmax(visibility.getXmax())
                                        .commandId(visibility.getCommandId())
                                        .activeXids(visibility.getActiveXids())
                        ) :
                constantScoreQuery(
                        boolQuery()
                                .must(query)
                                .mustNot(
                                        visibility()
                                                .myXid(visibility.getMyXid())
                                                .xmin(visibility.getXmin())
                                                .xmax(visibility.getXmax())
                                                .commandId(visibility.getCommandId())
                                                .activeXids(visibility.getActiveXids())
                                )
                );
    }
}
