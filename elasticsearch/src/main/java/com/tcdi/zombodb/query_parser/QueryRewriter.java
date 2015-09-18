/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015 ZomboDB, LLC
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
package com.tcdi.zombodb.query_parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
import java.util.*;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

public class QueryRewriter {

    private static enum DateHistogramIntervals {
        year, quarter, month, week, day, hour, minute, second
    }

    /* short for FilterBuilderFactory */
    private static interface FBF {
        public static FBF DUMMY = new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                throw new RuntimeException("Should not get here");
            }
        };

        FilterBuilder b(QueryParserNode n);
    }

    public static class QueryRewriteException extends RuntimeException {
        public QueryRewriteException(String message) {
            super(message);
        }

        public QueryRewriteException(Throwable cause) {
            super(cause);
        }
    }

    private static final String DateSuffix = ".date";

    private final Client client;
    private final RestRequest request;
    private final ASTQueryTree tree;
    private final QueryParserNode rootNode;
    private final String searchPreference;

    private final String input;
    private boolean allowSingleIndex;
    private boolean ignoreASTChild;
    private final boolean useParentChild;
    private boolean _isBuildingAggregate = false;
    private final boolean isRequestObjectAlive;
    private boolean queryRewritten = false;
    private ASTParent parentQuery;

    private Map<String, StringBuilder> arrayData;

    private final IndexMetadataManager metadataManager;

    static String toJson(String query) {
        return new QueryRewriter(null, null, query, true, true).rewriteQuery().toString();
    }

    static String dumpAsString(String query) throws Exception {
        return new QueryParser(new StringReader(query)).parse(true).dumpAsString();
    }

    public QueryRewriter(Client client, RestRequest request, String input, boolean allowSingleIndex, boolean useParentChild) {
        this(client, request, input, allowSingleIndex, false, useParentChild);
    }

    private QueryRewriter(Client client, RestRequest request, String input, boolean allowSingleIndex, boolean ignoreASTChild, boolean useParentChild) {
        this(client, request, input, allowSingleIndex, ignoreASTChild, useParentChild, false);
    }

    public  QueryRewriter(Client client, RestRequest request, String input, boolean allowSingleIndex, boolean ignoreASTChild, boolean useParentChild, boolean extractParentQuery) {
        this.client = client;
        this.request = request;
        this.input = input;
        this.allowSingleIndex = allowSingleIndex;
        this.ignoreASTChild = ignoreASTChild;
        this.useParentChild = useParentChild;
        this.searchPreference = request != null ? request.param("preference") : null;
        this.isRequestObjectAlive = request != null && request.getLocalAddress() != null;    // exists mainly for unit testing support because the request object is mocked and generally useless to us

        metadataManager = new IndexMetadataManager(
                client,
                request,
                new ASTIndexLink(QueryParserTreeConstants.JJTINDEXLINK) {
                    @Override
                    public String getLeftFieldname() {
                        return metadataManager == null || metadataManager.getMetadataForMyOriginalIndex() == null ? null : metadataManager.getMetadataForMyOriginalIndex().getPrimaryKeyFieldName();
                    }

                    @Override
                    public String getIndexName() {
                        return QueryRewriter.this.request.param("index");
                    }

                    @Override
                    public String getRightFieldname() {
                        return metadataManager == null ||  metadataManager.getMetadataForMyOriginalIndex() == null ? null : metadataManager.getMetadataForMyOriginalIndex().getPrimaryKeyFieldName();
                    }
                });

        try {
            final StringBuilder newQuery = new StringBuilder(input.length());
            QueryParser parser;
            arrayData = Utils.extractArrayData(input, newQuery);

            parser = new QueryParser(new StringReader(newQuery.toString()));
            tree = parser.parse(true);

            if (extractParentQuery) {
                parentQuery = (ASTParent) tree.getChild(ASTParent.class);
                tree.removeNode(parentQuery);
                tree.renumber();
                if (tree.getQueryNode() != null) {
                    tree.getQueryNode().removeNode(parentQuery);
                    tree.getQueryNode().renumber();
                }
            }

            // load index mappings for any index defined in #options()
            metadataManager.loadReferencedMappings(tree.getOptions());
            metadataManager.setUsedFields(parser.getUsedFieldnames());

            ASTAggregate aggregate = tree.getAggregate();
            ASTSuggest suggest = tree.getSuggest();
            if (aggregate != null || suggest != null) {
                String fieldname = aggregate != null ? aggregate.getFieldname() : suggest.getFieldname();
                final ASTIndexLink indexLink = metadataManager.findField(fieldname);
                if (indexLink != metadataManager.getMyIndex()) {
                    // change "myIndex" to that of the aggregate/suggest index
                    // so that we properly expand() the queries to do the right things
                    metadataManager.setMyIndex(indexLink);
                }
            }

            // now optimize the _all field into #expand()s, if any are in other indexes
            new IndexLinkOptimizer(tree, metadataManager).optimize();

            rootNode = useParentChild ? tree : tree.getChild(ASTChild.class);
            if (ignoreASTChild) {
                ASTChild child = (ASTChild) tree.getChild(ASTChild.class);
                if (child != null) {
                    ((QueryParserNode) child.parent).removeNode(child);
                    ((QueryParserNode) child.parent).renumber();
                }
            }

        } catch (ParseException pe) {
            throw new QueryRewriteException(pe);
        }
    }

    public String dumpAsString() {
        return tree.dumpAsString();
    }

    public Map<String, ?> describedNestedObject(String fieldname) throws Exception {
        return metadataManager.describedNestedObject(fieldname);
    }

    public IndexMetadataManager getMetadataManager() {
        return metadataManager;
    }

    public FilterBuilder rewriteParentQuery() {
        return parentQuery != null ? build(parentQuery) : null;
    }

    public QueryBuilder rewriteQuery() {
        try {
            return filteredQuery(matchAllQuery(), build(rootNode));
        } finally {
            queryRewritten = true;
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
            throw new RuntimeException("Cannot figure out which index to use for aggregation");
    }

    public String getAggregateFieldName() {
        String fieldname = tree.getAggregate().getFieldname();
        IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        if (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            if (base.equals(md.getLink().getFieldname()))   // strip base fieldname becase it's in a named index, not a json field
                fieldname = fieldname.substring(fieldname.indexOf('.')+1);
        }

        return fieldname;
    }

    public String getSearchIndexName() {
        if (!queryRewritten)
            throw new IllegalStateException("Must call .rewriteQuery() before calling .getSearchIndexName()");

        if (metadataManager.getUsedIndexes().size() == 1 && allowSingleIndex)
            return metadataManager.getUsedIndexes().iterator().next().getIndexName();
        else
            return metadataManager.getMyIndex().getIndexName();
    }

    private AbstractAggregationBuilder build(ASTAggregate agg) {
        if (agg == null)
            return null;

        AbstractAggregationBuilder ab;
        ASTAggregate subagg = agg.getSubAggregate();

        if (agg instanceof ASTTally)
            ab = build((ASTTally) agg);
        else if (agg instanceof ASTRangeAggregate)
            ab = build((ASTRangeAggregate) agg);
        else if (agg instanceof ASTSignificantTerms)
            ab = build((ASTSignificantTerms) agg);
        else if (agg instanceof ASTExtendedStats)
            ab = build((ASTExtendedStats) agg);
        else
            throw new RuntimeException("Unrecognized aggregation type: " + agg.getClass().getName());

        // TODO:  What if the sub aggregate is in a different index?
        if (subagg != null && ab instanceof AggregationBuilder)
            ((AggregationBuilder) ab).subAggregation(build(subagg));

        if (metadataManager.isFieldNested(agg.getFieldname())) {
            ab = nested(agg.getFieldname()).path(agg.getNestedPath())
                    .subAggregation(
                            filter(agg.getFieldname())
                                    .filter(build(tree))
                                    .subAggregation(ab).subAggregation(missing("missing").field(agg.getFieldname()))
                    );
        }

        return ab;
    }

    private TermSuggestionBuilder build(ASTSuggest agg) {
        if (agg == null)
            return null;

        TermSuggestionBuilder tsb = new TermSuggestionBuilder("suggestions");
        tsb.field(agg.getFieldname());
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

//        fieldname = getAggregateFieldName();

        boolean useHistogram = false;
        if (hasDate(md, fieldname)) {
            try {
                DateHistogramIntervals.valueOf(agg.getStem());
                useHistogram = true;
            } catch (IllegalArgumentException iae) {
                useHistogram = false;
            }
        }

        if (useHistogram) {
            DateHistogramBuilder dhb = dateHistogram(agg.getFieldname())
                    .field(agg.getFieldname() + DateSuffix)
                    .order(stringToDateHistogramOrder(agg.getSortOrder()));

            switch (DateHistogramIntervals.valueOf(agg.getStem())) {
                case year:
                    dhb.interval(DateHistogram.Interval.YEAR);
                    dhb.format("yyyy");
                    break;
                case month:
                    dhb.interval(DateHistogram.Interval.MONTH);
                    dhb.format("yyyy-MM");
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
                    throw new RuntimeException("Unsupported date histogram interval: " + agg.getStem());
            }

            return dhb;
        } else {
            TermsBuilder tb = terms(agg.getFieldname())
                    .field(fieldname)
                    .size(agg.getMaxTerms())
                    .shardSize(0)
                    .order(stringToTermsOrder(agg.getSortOrder()));

            if ("string".equalsIgnoreCase(md.getType(agg.getFieldname())))
                tb.include(agg.getStem());

            return tb;
        }
    }

    /**
     * Determine if a particular field name is present in the index
     *
     * @param md index metadata
     * @param fieldname field name to check for
     * @return true if this field exists, false otherwise
     */
    private boolean hasDate(final IndexMetadata md, final String fieldname) {
        return md.hasField(fieldname + DateSuffix);
    }

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

    private AggregationBuilder build(ASTRangeAggregate agg) {
        final String fieldname = agg.getFieldname();
        final IndexMetadata md = metadataManager.getMetadataForField(fieldname);

        try {
            ObjectMapper om = new ObjectMapper();

            // if this is a date field, execute a date range aggregation
            if (hasDate(md, fieldname)) {
                final DateRangeBuilder dateRangeBuilder = new DateRangeBuilder(fieldname)
                        .field(fieldname + DateSuffix);

                for (final DateRangeSpecEntry e : om.readValue(agg.getRangeSpec(), DateRangeSpecEntry[].class)) {
                    if (e.to == null && e.from == null)
                        throw new RuntimeException("Invalid range spec entry:  one of 'to' or 'from' must be specified");

                    if (e.from == null)
                        dateRangeBuilder.addUnboundedTo(e.key, e.to);
                    else if (e.to == null)
                        dateRangeBuilder.addUnboundedFrom(e.key, e.from);
                    else
                        dateRangeBuilder.addRange(e.key, e.from, e.to);
                }

                return dateRangeBuilder;
            }

            // this is not a date field so execute a normal numeric range aggregation
            final RangeBuilder rangeBuilder = new RangeBuilder(fieldname)
                    .field(fieldname);

            for (final RangeSpecEntry e : om.readValue(agg.getRangeSpec(), RangeSpecEntry[].class)) {
                if (e.to == null && e.from == null)
                    throw new RuntimeException("Invalid range spec entry:  one of 'to' or 'from' must be specified");

                if (e.from == null)
                    rangeBuilder.addUnboundedTo(e.key, e.to);
                else if (e.to == null)
                    rangeBuilder.addUnboundedFrom(e.key, e.from);
                else
                    rangeBuilder.addRange(e.key, e.from, e.to);
            }

            return rangeBuilder;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private AggregationBuilder build(ASTSignificantTerms agg) {
        IndexMetadata md = metadataManager.getMetadataForMyIndex();
        SignificantTermsBuilder stb = significantTerms(agg.getFieldname())
                .field(agg.getFieldname())
                .size(agg.getMaxTerms());

        if ("string".equalsIgnoreCase(md.getType(agg.getFieldname())))
            stb.include(agg.getStem());

        return stb;
    }

    private AbstractAggregationBuilder build(ASTExtendedStats agg) {
        return extendedStats(agg.getFieldname())
                .field(agg.getFieldname());
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

    private FilterBuilder build(QueryParserNode node) {
        if (node == null)
            return null;
        return build(node, metadataManager.getMyIndex());
    }

    private FilterBuilder build(QueryParserNode node, ASTIndexLink link) {
        if (node instanceof ASTChild)
            return build((ASTChild) node);
        else if (node instanceof ASTParent)
            return build((ASTParent) node);
        else if (node instanceof ASTNested)
            return build((ASTNested) node);
        else if (node instanceof ASTAnd)
            return build((ASTAnd) node);
        else if (node instanceof ASTNestedGroup)
            return build((ASTNestedGroup) node);
        else if (node instanceof ASTNot)
            return build((ASTNot) node);
        else if (node instanceof ASTOr)
            return build((ASTOr) node);

        return build0(node);
    }

    private FilterBuilder build0(QueryParserNode node) {
        if (node instanceof ASTArray)
            return build((ASTArray) node);
        else if (node instanceof ASTArrayData)
            return build((ASTArrayData) node);
        else if (node instanceof ASTBoolean)
            return build((ASTBoolean) node);
        else if (node instanceof ASTFuzzy)
            return build((ASTFuzzy) node);
        else if (node instanceof ASTNotNull)
            return build((ASTNotNull) node);
        else if (node instanceof ASTNull)
            return build((ASTNull) node);
        else if (node instanceof ASTNumber)
            return build((ASTNumber) node);
        else if (node instanceof ASTPhrase)
            return build((ASTPhrase) node);
        else if (node instanceof ASTPrefix)
            return build((ASTPrefix) node);
        else if (node instanceof ASTProximity)
            return build((ASTProximity) node);
        else if (node instanceof ASTQueryTree)
            return build((ASTQueryTree) node);
        else if (node instanceof ASTRange)
            return build((ASTRange) node);
        else if (node instanceof ASTWildcard)
            return build((ASTWildcard) node);
        else if (node instanceof ASTWord)
            return build((ASTWord) node);
        else if (node instanceof ASTScript)
            return build((ASTScript) node);
        else if (node instanceof ASTExpansion)
            return build((ASTExpansion) node);
        else
            throw new QueryRewriteException("Unexpected node type: " + node.getClass().getName());
    }

    private FilterBuilder build(ASTQueryTree root) throws QueryRewriteException {
        QueryParserNode queryNode = root.getQueryNode();

        if (queryNode == null)
            return matchAllFilter();

        // and build the query
        return build(queryNode);
    }

    private FilterBuilder build(ASTAnd node) {
        BoolFilterBuilder fb = boolFilter();

        for (QueryParserNode child : node) {
            fb.must(build(child));
        }
        return fb;
    }


    private FilterBuilder build(ASTOr node) {
        BoolFilterBuilder fb = boolFilter();

        for (QueryParserNode child : node) {
            fb.should(build(child));
        }
        return fb;
    }

    private FilterBuilder build(ASTNot node) {
        BoolFilterBuilder qb = boolFilter();

        if (_isBuildingAggregate)
            return matchAllFilter();

        for (QueryParserNode child : node) {
            qb.mustNot(build(child));
        }
        return qb;
    }

    private FilterBuilder build(ASTNestedGroup node) {

        if (metadataManager.isNestedGroupExternal(node)) {
            String base = node.getFieldname();
            QueryParserNode finalNode = node.getChild(0);

            if (node.jjtGetNumChildren() > 1) {
                ASTAnd and = new ASTAnd(QueryParserTreeConstants.JJTAND);
                and.adoptChildren(node);
                finalNode = and;
            }

            ASTExpansion expansion = new ASTExpansion(QueryParserTreeConstants.JJTEXPANSION);
            expansion.jjtAddChild(metadataManager.getExternalIndexLink(base), 0);
            expansion.jjtAddChild(finalNode, 1);
            rewriteFieldnames(expansion, base);

            return expand(expansion, expansion.getIndexLink());
        } else {

            if (node.jjtGetNumChildren() == 1)
                return nestedFilter(node.getNestedPath(), build(node.getChild(0))).join(!_isBuildingAggregate);
            else {
                BoolFilterBuilder fb = boolFilter();
                for (QueryParserNode child : node) {
                    if (node.isAnd())
                        fb.must(build(child));
                    else
                        fb.should(build(child));
                }
                return nestedFilter(node.getNestedPath(), fb).join(!_isBuildingAggregate);
            }
        }
    }

    private void rewriteFieldnames(QueryParserNode node, String base) {
        if (node.fieldname != null && node.fieldname.startsWith(base + "."))
            node.fieldname = node.fieldname.substring(node.fieldname.indexOf('.') + 1);

        for (QueryParserNode child : node)
            rewriteFieldnames(child, base);
    }

    private FilterBuilder build(ASTChild node) {
        if (node.hasChildren() && !ignoreASTChild) {
            if (useParentChild) {
                return hasChildFilter(node.getTypename(), build(node.getChild(0)));
            } else {
                return build(node.getChild(0));
            }
        } else {
            return matchAllFilter();
        }
    }

    private FilterBuilder build(ASTParent node) {
        if (_isBuildingAggregate)
            return matchAllFilter();
        else if (node.hasChildren())
            return hasParentFilter(node.getTypename(), build(node.getChild(0)));
        else
            return matchAllFilter();
    }

    private String nested = null;

    private FilterBuilder build(ASTNested node) {
        if (node.hasChildren()) {
            try {
                nested = node.getTypename();
                return nestedFilter(node.getTypename(), build(node.getChild(0))).join(!_isBuildingAggregate);
            } finally {
                nested = null;
            }
        } else
            return matchAllFilter();
    }

    private Stack<ASTExpansion> generatedExpansionsStack = new Stack<>();

    private FilterBuilder build(final ASTExpansion node) {
        final ASTIndexLink link = node.getIndexLink();

        try {
            if (node.isGenerated())
                generatedExpansionsStack.push(node);

            return expand(node, link);
        } finally {
            if (node.isGenerated())
                generatedExpansionsStack.pop();
        }
    }

    private boolean needsConversionToPhrase(String value, String fieldname) {
        IndexMetadata metadata = metadataManager.getMetadataForField(fieldname);
        String analyzer = metadata != null ? metadata.getAnalyzer(fieldname) : null;

        return !(analyzer == null || "exact".equals(analyzer)) && Utils.isComplexTerm(value);
    }

    private FilterBuilder build(ASTWord node) {
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                Object value = n.getValue();

                // if the value is a string and the field is not indexed as "exact"
                // and if the value contains any non-alphanums, we want to execute a phrase
                // query, despite the fact that the value was parsed as an ASTWord
                if (value instanceof String) {
                    if (("_all".equals(n.getFieldname()) && Utils.isComplexTerm((String) value)) || needsConversionToPhrase((String) value, n.getFieldname()))
                        return queryFilter(matchPhraseQuery(n.getFieldname(), value));
                }

                return termFilter(n.getFieldname(), value);
            }
        });
    }

    private FilterBuilder build(ASTScript node) {
        return scriptFilter(node.getValue().toString());
    }

    private FilterBuilder build(ASTPhrase node) {
        if (node.getOperator() == QueryParserNode.Operator.REGEX)
            return buildStandard(node, FBF.DUMMY);
        IndexMetadata md = metadataManager.getMetadataForField(node.getFieldname());
        final String analyzer = md == null ? null : md.getAnalyzer(node.getFieldname());

        String value = String.valueOf(node.getValue());

        if (node.getOperator() != QueryParserNode.Operator.REGEX && !node.isBeenSubparsed() && (!Utils.hasOnlyEscapedWildcards(node) && value.contains("*") || value.contains("?") || value.contains("~") || node.getDistance() != 0)) {
            // phrase contains wildcards or has a non-default distance value so we need to build as a span
            // but this only supports a few operators
            switch (node.getOperator()) {
                case EQ:
                case NE:
                case CONTAINS:
                    break;
                default:
                    throw new QueryRewriteException("Unsupported operator for PHRASE_WITH_WILDCARD value: " + node.getOperator());
            }

            if (!Utils.hasOnlyEscapedWildcards(node) && "exact".equals(analyzer) && (value.contains("*") || value.contains("?") || value.contains("~"))) {
                // it's an exact-indexed field and the phrase node contains a wildcard

                if (Utils.countValidWildcards(node) == 1 && (value.endsWith("*") || value.endsWith("?") || value.endsWith("~"))) {

                    String escaped = node.getEscapedValue().substring(0, node.getEscapedValue().length()-1);

                    if (value.endsWith("~")) {
                        // it's a fuzzy
                        ASTFuzzy fuzzy = new ASTFuzzy(QueryParserTreeConstants.JJTFUZZY);
                        fuzzy.setFieldname(node.getFieldname());
                        fuzzy.value = escaped;
                        fuzzy.operator = node.getOperator();
                        return maybeNest(fuzzy, build(fuzzy));
                    } else {
                        // it's a prefix
                        ASTPrefix prefix = new ASTPrefix(QueryParserTreeConstants.JJTPREFIX);
                        prefix.setFieldname(node.getFieldname());
                        prefix.value = escaped;
                        prefix.operator = node.getOperator();
                        return maybeNest(prefix, build(prefix));
                    }
                }

                ASTWildcard wildcard = new ASTWildcard(QueryParserTreeConstants.JJTWILDCARD);
                wildcard.setFieldname(node.getFieldname());
                wildcard.value = node.getEscapedValue();
                wildcard.operator = node.getOperator();
                return maybeNest(wildcard, build(wildcard));
            }

            ASTProximity prox = new ASTProximity(QueryParserTreeConstants.JJTPROXIMITY);
            prox.fieldname = node.getFieldname();
            prox.boost = node.getBoost();
            prox.distance = node.getDistance();
            prox.ordered = node.isOrdered();

            return maybeNest(node, buildSpanOrFilter(prox, node));
        } else {
            return buildStandard(node, new FBF() {
                @Override
                public FilterBuilder b(QueryParserNode n) {

                    Utils.subparsePhrase(n.getEscapedValue(), "foo", n.getOperator());
                    if (!n.getValue().toString().contains(" ") || (!n.getFieldname().equals("_all") && (analyzer == null || "exact".equals(analyzer))))
                        return termFilter(n.getFieldname(), n.getValue());
                    else
                        return queryFilter(matchPhraseQuery(n.getFieldname(), n.getValue()));
                }
            });
        }
    }

    private FilterBuilder build(ASTNumber node) {
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                return termFilter(n.getFieldname(), n.getValue());
            }
        });
    }

    private FilterBuilder build(ASTNull node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for NULL value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                return missingFilter(n.getFieldname());
            }
        });
    }

    private FilterBuilder build(ASTNotNull node) {
        IndexMetadata md = metadataManager.getMetadataForField(node.getFieldname());
        if (md != null && node.getFieldname().equalsIgnoreCase(md.getPrimaryKeyFieldName()))
            return matchAllFilter();    // optimization when we know every document has a value for the specified field

        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for NOT NULL value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                return existsFilter(n.getFieldname());
            }
        });
    }

    private FilterBuilder build(ASTBoolean node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for BOOLEAN value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                return termFilter(n.getFieldname(), n.getValue());
            }
        });
    }

    private FilterBuilder build(final ASTFuzzy node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for FUZZY value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                if (needsConversionToPhrase(String.valueOf(node.getValue()), node.getFieldname())) {
                    ASTProximity prox = new ASTProximity(QueryParserTreeConstants.JJTPROXIMITY);
                    ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);

                    prox.fieldname = n.fieldname;
                    prox.ordered = true;
                    prox.distance = 0;

                    phrase.value = Utils.join(Utils.simpleTokenize(n.getValue() + "~" + node.fuzzyness));
                    phrase.fieldname = n.fieldname;
                    phrase.ordered = true;
                    phrase.operator = n.getOperator();

                    return buildSpanOrFilter(prox, phrase);
                }

                return queryFilter(fuzzyQuery(n.getFieldname(), n.getValue()).prefixLength(n.getFuzzyness() == 0 ? 3 : n.getFuzzyness()));
            }
        });
    }

    private FilterBuilder build(final ASTPrefix node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for PREFIX value: " + node.getOperator());
        }

        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                if (needsConversionToPhrase(String.valueOf(node.getValue()), node.getFieldname())) {
                    ASTProximity prox = new ASTProximity(QueryParserTreeConstants.JJTPROXIMITY);
                    ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);

                    prox.fieldname = n.fieldname;
                    prox.ordered = true;
                    prox.distance = 0;

                    phrase.value = Utils.join(Utils.simpleTokenize(n.getValue() + "*"));
                    phrase.fieldname = n.fieldname;
                    phrase.ordered = true;
                    phrase.operator = n.getOperator();

                    return buildSpanOrFilter(prox, phrase);
                }

                return prefixFilter(n.getFieldname(), String.valueOf(n.getValue()));
            }
        });
    }

    private FilterBuilder build(final ASTWildcard node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for WILDCARD value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                if (needsConversionToPhrase(String.valueOf(node.getValue()), node.getFieldname())) {
                    ASTProximity prox = new ASTProximity(QueryParserTreeConstants.JJTPROXIMITY);
                    ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);

                    prox.fieldname = n.fieldname;
                    prox.ordered = true;
                    prox.distance = 0;

                    phrase.value = Utils.join(Utils.simpleTokenize(String.valueOf(n.getValue())));
                    phrase.fieldname = n.fieldname;
                    phrase.ordered = true;
                    phrase.operator = n.getOperator();

                    return buildSpanOrFilter(prox, phrase);
                }

                return queryFilter(wildcardQuery(n.getFieldname(), String.valueOf(n.getValue())));
            }
        });
    }

    private FilterBuilder build(final ASTArray node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for ARRAY value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                boolean canUseFieldData = node.hasExternalValues() && node.getTotalExternalValues() >= 10000 && metadataManager.getMetadataForField(n.getFieldname()).canUseFieldData(n.getFieldname());
                boolean isNE = node.getOperator() == QueryParserNode.Operator.NE;

                // NB:  testing shows that "fielddata" is *significantly* faster for large number of terms, about 2x faster than "plain"
                return termsFilter(n.getFieldname(), node.hasExternalValues() ? node.getExternalValues() : n.getChildValues())
                        .execution( (node.isAnd() && !isNE) || (!node.isAnd() && isNE) ? "and" : canUseFieldData ? "fielddata" : "plain");
            }
        });
    }

    private FilterBuilder build(final ASTArrayData node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for ARRAY value: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                boolean canUseFieldData = metadataManager.getMetadataForField(n.getFieldname()).canUseFieldData(n.getFieldname());

                // NB:  testing shows that "fielddata" is *significantly* faster for large number of terms, about 2x faster than "plain"
                return termsFilter(n.getFieldname(), new Iterable<String>() {
                    @Override
                    public Iterator<String> iterator() {
                        final EscapingStringTokenizer st = new EscapingStringTokenizer(arrayData.get(node.value.toString()).toString(), ", \r\n\t\f\"'[]");
                        return new Iterator<String>() {
                            @Override
                            public boolean hasNext() {
                                return st.hasMoreTokens();
                            }

                            @Override
                            public String next() {
                                return st.nextToken();
                            }

                            @Override
                            public void remove() {

                            }
                        };
                    }
                }).execution(canUseFieldData ? "fielddata" : "plain");
            }
        });
    }

    private FilterBuilder build(final ASTRange node) {
        switch (node.getOperator()) {
            case EQ:
            case NE:
            case CONTAINS:
                break;
            default:
                throw new QueryRewriteException("Unsupported operator for RANGE queries: " + node.getOperator());
        }
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                QueryParserNode start = n.getChild(0);
                QueryParserNode end = n.getChild(1);
                return rangeFilter(node.getFieldname()).from(start.getValue()).to(end.getValue());
            }
        });
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, QueryParserNode node) {
        if (node instanceof ASTWord)
            return buildSpan(prox, (ASTWord) node);
        else if (node instanceof ASTNumber)
            return buildSpan(prox, (ASTNumber) node);
        else if (node instanceof ASTBoolean)
            return buildSpan(prox, (ASTBoolean) node);
        else if (node instanceof ASTFuzzy)
            return buildSpan(prox, (ASTFuzzy) node);
        else if (node instanceof ASTPrefix)
            return buildSpan(prox, (ASTPrefix) node);
        else if (node instanceof ASTWildcard)
            return buildSpan(prox, (ASTWildcard) node);
        else if (node instanceof ASTPhrase)
            return buildSpan(prox, (ASTPhrase) node);
        else if (node instanceof ASTNull)
            return buildSpan(prox, (ASTNull) node);
        else if (node instanceof ASTNotNull)
            return buildSpan(prox, (ASTNotNull) node);
        else if (node instanceof ASTProximity)
            return buildSpan((ASTProximity) node);
        else
            throw new QueryRewriteException("Unsupported PROXIMITY node: " + node.getClass().getName());
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
        if (prox.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        if (needsConversionToPhrase(String.valueOf(node.getEscapedValue()), prox.getFieldname())) {
            ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);
            String value = String.valueOf(node.getEscapedValue());

            phrase.fieldname = node.fieldname;
            phrase.value = Utils.join(Utils.simpleTokenize(value));
            phrase.ordered = true;
            phrase.operator = node.getOperator();
            return buildSpan(prox, phrase);
        }
        return spanTermQuery(prox.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNull node) {
        // when building spans, treat 'null' as a regular term
        return spanTermQuery(prox.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNotNull node) {
        throw new QueryRewriteException("Bare wildcards not supported within phrases");
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNumber node) {
        return spanTermQuery(prox.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTBoolean node) {
        return spanTermQuery(prox.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTFuzzy node) {
        return spanMultiTermQueryBuilder(fuzzyQuery(prox.getFieldname(), node.getValue()).prefixLength(node.getFuzzyness() == 0 ? 3 : node.getFuzzyness()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTPrefix node) {
        if (needsConversionToPhrase(String.valueOf(node.getEscapedValue()), prox.getFieldname())) {
            ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);
            String value = String.valueOf(node.getEscapedValue()) + "*";

            phrase.fieldname = node.fieldname;
            phrase.value = Utils.join(Utils.simpleTokenize(value));
            phrase.ordered = true;
            phrase.operator = node.getOperator();
            return buildSpan(prox, phrase);
        }

        return spanMultiTermQueryBuilder(prefixQuery(prox.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTWildcard node) {
        return spanMultiTermQueryBuilder(wildcardQuery(prox.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTPhrase node) {
        if (prox.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        SpanNearQueryBuilder qb = spanNearQuery();

        for (QueryParserNode child : subparsePhrase(node).nodes)
            qb.clause(buildSpan(prox, child));

        qb.slop(0);
        qb.inOrder(true);

        return qb;
    }

    private FilterBuilder buildSpanOrFilter(ASTProximity prox, ASTPhrase node) {
        SpanNearQueryBuilder qb = spanNearQuery();
        Utils.SubparseInfo info = subparsePhrase(node);

        if (info.totalCount == 1) {
            return build(info.nodes.iterator().next());
        } else {
            for (QueryParserNode child : info.nodes) {
                if ("-".equals(child.getValue()))
                    continue;
                else if ("+".equals(child.getValue()))
                    continue;

                qb.clause(buildSpan(prox, child));
            }

            qb.slop(node.getDistance());
            qb.inOrder(node.isOrdered());

            return queryFilter(qb);
        }
    }


    private Utils.SubparseInfo subparsePhrase(final ASTPhrase phrase) {
        boolean needsSpan = !phrase.isOrdered() || phrase.getDistance() != 0 || !Utils.hasOnlyEscapedWildcards(phrase) || needsConversionToPhrase(String.valueOf(phrase.getEscapedValue()), phrase.getFieldname());
        boolean beenParsedAlready = phrase.isBeenSubparsed();

        phrase.setBeenSubparsed(true);

        if (!needsSpan) {
            if (beenParsedAlready) {
                ASTWord word = new ASTWord(QueryParserTreeConstants.JJTWORD);
                word.fieldname = phrase.fieldname;
                word.value = phrase.value;
                word.operator = phrase.getOperator();

                // the phrase does not actually need to be subparsed, so just return it as-is
                return new Utils.SubparseInfo(Arrays.asList((QueryParserNode) word), 1, phrase.getOperator());
            } else {
                return new Utils.SubparseInfo(Arrays.asList((QueryParserNode) phrase), 1, phrase.getOperator());
            }
        }


        return Utils.subparsePhrase(phrase.getEscapedValue(), phrase.getFieldname(), phrase.getOperator());
    }

    private FilterBuilder build(ASTProximity node) {
        node.forceFieldname(node.getFieldname());

        SpanNearQueryBuilder qb = spanNearQuery();
        qb.slop(node.getDistance());
        qb.inOrder(node.isOrdered());

        for (QueryParserNode child : node) {
            qb.clause(buildSpan(node, child));
        }

        return queryFilter(qb);
    }

    private FilterBuilder buildStandard(QueryParserNode node, FBF fbf) {
        return maybeNest(node, buildStandard0(node, fbf));
    }

    private FilterBuilder buildStandard0(QueryParserNode node, FBF fbf) {
        switch (node.getOperator()) {
            case EQ:
            case CONTAINS:
                return fbf.b(node);

            case NE:
                return notFilter(fbf.b(node));

            case LT:
                return rangeFilter(node.getFieldname()).lt(node.getValue());
            case GT:
                return rangeFilter(node.getFieldname()).gt(node.getValue());
            case LTE:
                return rangeFilter(node.getFieldname()).lte(node.getValue());
            case GTE:
                return rangeFilter(node.getFieldname()).gte(node.getValue());

            case REGEX:
                return regexpFilter(node.getFieldname(), node.getEscapedValue());

            case CONCEPT: {
                int minTermFreq = 2;
                // drop the minTermFreq to 1 if we
                // determine that the field being queried is NOT of type "fulltext"
                IndexMetadata md = metadataManager.getMetadataForField(node.getFieldname());
                if (md != null)
                    if (!"fulltext".equalsIgnoreCase(md.getAnalyzer(node.getFieldname())))
                        minTermFreq = 1;

                return queryFilter(moreLikeThisQuery(node.getFieldname()).likeText(String.valueOf(node.getValue())).maxQueryTerms(80).minWordLength(3).minTermFreq(minTermFreq).stopWords(IndexMetadata.MLT_STOP_WORDS));
            }

            case FUZZY_CONCEPT:
                return queryFilter(fuzzyLikeThisFieldQuery(node.getFieldname()).likeText(String.valueOf(node.getValue())).maxQueryTerms(80).fuzziness(Fuzziness.AUTO));

            default:
                throw new RuntimeException("Unexpected operator: " + node.getOperator());
        }
    }

    private FilterBuilder maybeNest(QueryParserNode node, FilterBuilder qb) {
        if (!node.isNested()) {
            if (_isBuildingAggregate)
                return matchAllFilter();
            return qb;  // it's not nested, so just return
        }


        if (nested != null) {
            // we are currently nesting, so make sure this node's path
            // matches the one we're in
            if (node.getNestedPath().equals(nested))
                return qb;  // since we're already nesting, no need to do anything
            else
                throw new RuntimeException("Attempt to use nested path '" + node.getNestedPath() + "' inside '" + nested + "'");
        }

        return qb;
    }

    private FilterBuilder makeParentFilter(ASTExpansion node) {
        if (ignoreASTChild)
            return null;

        ASTIndexLink link = node.getIndexLink();
        IndexMetadata md = metadataManager.getMetadata(link);
        if (md != null && md.getNoXact())
            return null;

        QueryRewriter qr = new QueryRewriter(client, request, input, allowSingleIndex, true, true);
        QueryParserNode parentQuery = qr.tree.getChild(ASTParent.class);
        if (parentQuery != null) {
            return build(parentQuery);
        } else {
            return hasParentFilter("xact", qr.rewriteQuery());
        }
    }

    private Stack<ASTExpansion> buildExpansionStack(QueryParserNode root, Stack<ASTExpansion> stack) {

        if (root != null) {
            if (root instanceof ASTExpansion) {
                stack.push((ASTExpansion) root);
                buildExpansionStack(((ASTExpansion) root).getQuery(), stack);
            } else {
                for (QueryParserNode child : root)
                    buildExpansionStack(child, stack);
            }
        }
        return stack;
    }

    private QueryParserNode loadFielddata(ASTExpansion node, String leftFieldname, String rightFieldname) {
        ASTIndexLink link = node.getIndexLink();
        QueryParserNode nodeQuery = node.getQuery();
        IndexMetadata nodeMetadata = metadataManager.getMetadata(link);
        IndexMetadata leftMetadata = metadataManager.getMetadataForField(leftFieldname);
        IndexMetadata rightMetadata = metadataManager.getMetadataForField(rightFieldname);
        boolean isPkey = nodeMetadata != null && leftMetadata != null && rightMetadata != null &&
                nodeMetadata.getPrimaryKeyFieldName().equals(nodeQuery.getFieldname()) && leftMetadata.getPrimaryKeyFieldName().equals(leftFieldname) && rightMetadata.getPrimaryKeyFieldName().equals(rightFieldname);

        if (nodeQuery instanceof ASTNotNull && isPkey) {
            // if the query is a "not null" query against a primary key field and is targeting a primary key field
            // we can just rewrite the query as a "not null" query against the leftFieldname
            // and avoid doing a search at all
            ASTNotNull notNull = new ASTNotNull(QueryParserTreeConstants.JJTNOTNULL);
            notNull.setFieldname(leftFieldname);
            return notNull;
        }

        FilterBuilder nodeFilter = build(nodeQuery);
        SearchRequestBuilder builder = new SearchRequestBuilder(client)
                .setSize(10240)
                .setQuery(constantScoreQuery(nodeFilter))
                .setIndices(link.getIndexName())
                .setTypes("data")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10))
                .addFieldDataField(rightFieldname)
                .setPostFilter(makeParentFilter(node))
                .setPreference(searchPreference);

        ActionFuture<SearchResponse> future = client.search(builder.request());
        try {
            ASTArray array = new ASTArray(QueryParserTreeConstants.JJTARRAY);
            array.setFieldname(leftFieldname);

            SearchResponse response = future != null ? future.get() : null;
            long totalHits = response == null ? -1 : response.getHits().getTotalHits();

            if (response == null || totalHits == 0) {
                return array;
            } else if (response.getFailedShards() > 0) {
                StringBuilder sb = new StringBuilder();
                for (ShardSearchFailure failure : response.getShardFailures()) {
                    sb.append(failure).append("\n");
                }
                throw new RuntimeException(response.getFailedShards() + " shards failed:\n" + sb);
            }

            Set<Object> values = new TreeSet<>();
            int cnt = 0;
            while (cnt != totalHits) {
                response = client.searchScroll(new SearchScrollRequestBuilder(client)
                        .setScrollId(response.getScrollId())
                        .setScroll(TimeValue.timeValueSeconds(10))
                        .request()).get();

                if (response.getTotalShards() != response.getSuccessfulShards())
                    throw new Exception(response.getTotalShards() - response.getSuccessfulShards() + " shards failed");

                SearchHits hits = response.getHits();
                for (SearchHit hit : hits) {
                    List l = hit.field(rightFieldname).getValues();
                    if (l != null)
                        values.addAll(l);
                }
                cnt += hits.hits().length;
            }
            array.setExternalValues(values, cnt);

            return array;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FilterBuilder expand(final ASTExpansion root, final ASTIndexLink link) {
        if (!isRequestObjectAlive)
            return build(root.getQuery());
        Stack<ASTExpansion> stack = buildExpansionStack(root, new Stack<ASTExpansion>());

        ASTIndexLink myIndex = metadataManager.getMyIndex();
        ASTIndexLink targetIndex = !generatedExpansionsStack.isEmpty() ? root.getIndexLink() : myIndex;
        QueryParserNode last = null;

        if (link.getFieldname() != null)
            IndexLinkOptimizer.stripPath(root, link.getFieldname());

        try {
            while (!stack.isEmpty()) {
                ASTExpansion expansion = stack.pop();
                String expansionFieldname = expansion.getFieldname();

                if (expansionFieldname == null)
                    expansionFieldname = expansion.getIndexLink().getRightFieldname();

                if (generatedExpansionsStack.isEmpty() && expansion.getIndexLink() == myIndex) {
                    last = expansion.getQuery();
                } else {
                    String leftFieldname;
                    String rightFieldname;

                    if (expansion.isGenerated()) {

                        if (last == null) {
                            last = loadFielddata(expansion, expansion.getIndexLink().getLeftFieldname(), expansion.getIndexLink().getRightFieldname());
                        }

                        // at this point 'expansion' represents the set of records that match the #expand<>(...)'s subquery
                        // all of which are targeted towards the index that contains the #expand's <fieldname>

                        // the next step is to turn them into a set of 'expansionField' values
                        // then turn that around into a set of ids against myIndex, if the expansionField is not in myIndex
                        last = loadFielddata(expansion, expansion.getIndexLink().getLeftFieldname(), expansion.getIndexLink().getRightFieldname());

                        ASTIndexLink expansionSourceIndex = metadataManager.findField(expansionFieldname);
                        if (expansionSourceIndex != myIndex) {
                            // replace the ASTExpansion in the tree with the fieldData version
                            expansion.jjtAddChild(last, 1);

                            String targetPkey = myIndex.getRightFieldname();
                            String sourcePkey = metadataManager.getMetadata(expansion.getIndexLink().getIndexName()).getPrimaryKeyFieldName();

                            leftFieldname = targetPkey;
                            rightFieldname = sourcePkey;

                            last = loadFielddata(expansion, leftFieldname, rightFieldname);
                        }
                    } else {

                        List<String> path = metadataManager.calculatePath(targetIndex, expansion.getIndexLink());

                        boolean oneToOne = true;
                        if (path.size() == 2) {
                            leftFieldname = path.get(0);
                            rightFieldname = path.get(1);

                            leftFieldname = leftFieldname.substring(leftFieldname.indexOf(':') + 1);
                            rightFieldname = rightFieldname.substring(rightFieldname.indexOf(':') + 1);

                        } else if (path.size() == 3) {
                            oneToOne = false;
                            String middleFieldname;

                            leftFieldname = path.get(0);
                            middleFieldname = path.get(1);
                            rightFieldname = path.get(2);

                            if (metadataManager.areFieldPathsEquivalent(leftFieldname, middleFieldname) && metadataManager.areFieldPathsEquivalent(middleFieldname, rightFieldname)) {
                                leftFieldname = leftFieldname.substring(leftFieldname.indexOf(':') + 1);
                                rightFieldname = rightFieldname.substring(rightFieldname.indexOf(':') + 1);
                            } else {
                                throw new RuntimeException("Field equivalency cannot be determined");
                            }
                        } else {
                            // although I think we can with a while() loop that keeps resolving field data with each
                            // node in the path
                            throw new RuntimeException("Don't know how to resolve multiple levels of indirection");
                        }

                        if (oneToOne && metadataManager.getUsedIndexes().size() == 1 && allowSingleIndex) {
                            last = expansion.getQuery();
                        } else {
                            last = loadFielddata(expansion, leftFieldname, rightFieldname);
                        }
                    }
                }

                // replace the ASTExpansion in the tree with the fieldData version
                ((QueryParserNode) expansion.parent).replaceChild(expansion, last);
            }
        } finally {
            metadataManager.setMyIndex(myIndex);
        }

        return build(last);
    }
}
