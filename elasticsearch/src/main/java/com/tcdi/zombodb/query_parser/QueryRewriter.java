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

    public static String[] WILDCARD_TOKENS = {"ZDB_ESCAPE_ZDB", "ZDB_STAR_ZDB", "ZDB_QUESTION_ZDB", "ZDB_TILDE_ZDB"};
    public static String[] WILDCARD_VALUES = {"\\\\",           "*",            "?",                "~"};

    private static enum DateHistogramIntervals {
        year, quarter, month, week, day, hour, minute, second
    }

    /* short for FilterBuilderFactory */
    private static interface FBF {
        public static FBF DUMMY = new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                throw new QueryRewriteException("Should not get here");
            }
        };

        FilterBuilder b(QueryParserNode n);
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

    private final Client client;
    private final ASTQueryTree tree;
    private final QueryParserNode rootNode;
    private final String searchPreference;

    private final String indexName;
    private final String input;
    private boolean allowSingleIndex;
    private boolean ignoreASTChild;
    private final boolean useParentChild;
    private boolean _isBuildingAggregate = false;
    private boolean queryRewritten = false;
    private ASTParent parentQuery;

    private Map<String, StringBuilder> arrayData;

    private final IndexMetadataManager metadataManager;

    static String dumpAsString(String query) throws Exception {
        return new QueryParser(new StringReader(query)).parse(true).dumpAsString();
    }

    public QueryRewriter(Client client, String indexName, String searchPreference, String input, boolean allowSingleIndex, boolean useParentChild) {
        this(client, indexName, searchPreference, input, allowSingleIndex, false, useParentChild);
    }

    private QueryRewriter(Client client, String indexName, String searchPreference, String input, boolean allowSingleIndex, boolean ignoreASTChild, boolean useParentChild) {
        this(client, indexName, searchPreference, input, allowSingleIndex, ignoreASTChild, useParentChild, false);
    }

    public  QueryRewriter(Client client, final String indexName, String searchPreference, String input, boolean allowSingleIndex, boolean ignoreASTChild, boolean useParentChild, boolean extractParentQuery) {
        this.client = client;
        this.indexName = indexName;
        this.input = input;
        this.allowSingleIndex = allowSingleIndex;
        this.ignoreASTChild = ignoreASTChild;
        this.useParentChild = useParentChild;
        this.searchPreference = searchPreference;

        metadataManager = new IndexMetadataManager(
                client,
                new ASTIndexLink(QueryParserTreeConstants.JJTINDEXLINK) {
                    @Override
                    public String getLeftFieldname() {
                        return metadataManager == null || metadataManager.getMetadataForMyOriginalIndex() == null ? null : metadataManager.getMetadataForMyOriginalIndex().getPrimaryKeyFieldName();
                    }

                    @Override
                    public String getIndexName() {
                        return indexName;
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
                    throw new QueryRewriteException("Unsupported date histogram interval: " + agg.getStem());
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
                    .field(fieldname + DateSuffix);

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
                    .field(fieldname);

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
        else if (node instanceof ASTAnd)
            return build((ASTAnd) node);
        else if (node instanceof ASTWith)
            return build((ASTWith) node);
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

    private int withDepth = 0;
    private String withNestedPath;
    private FilterBuilder build(ASTWith node) {
        if (withDepth == 0)
            withNestedPath = Utils.validateSameNestedPath(node);

        BoolFilterBuilder fb = boolFilter();

        withDepth++;
        try {
            for (QueryParserNode child : node) {
                fb.must(build(child));
            }
        } finally {
            withDepth--;
        }

        return withDepth == 0 ? nestedFilter(withNestedPath, fb).join(shouldJoinNestedFilter()) : fb;
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

    private FilterBuilder build(ASTWord node) {
        return buildStandard(node, new FBF() {
            @Override
            public FilterBuilder b(QueryParserNode n) {
                Object value = n.getValue();
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

        final List<String> tokens = Utils.tokenizePhrase(client, metadataManager, node.getFieldname(), node.getEscapedValue());
        boolean hasWildcards = node.getDistance() > 0 || !node.isOrdered();
        for (int i=0; i<tokens.size(); i++) {
            String token = tokens.get(i);

            for (int j=0; j<WILDCARD_TOKENS.length; j++) {
                String wildcard = WILDCARD_TOKENS[j];
                String replacement = WILDCARD_VALUES[j];

                if (token.contains(wildcard)) {
                    tokens.set(i, token = token.replaceAll(wildcard, replacement));
                }
            }

            hasWildcards |= Utils.countValidWildcards(token) > 0;
        }

        for (Iterator<String> itr = tokens.iterator(); itr.hasNext();) {
            String token = itr.next();
            if (token.length() == 1) {
                if (token.equals("\\"))
                    itr.remove();
            }
        }

        if (hasWildcards) {
            if (tokens.size() == 1) {
                return build(Utils.convertToWildcardNode(node.getFieldname(), node.getOperator(), tokens.get(0)));
            } else {

                // convert list of tokens into a proximity query,
                // parse it into a syntax tree
                ASTProximity prox = new ASTProximity(QueryParserTreeConstants.JJTPROXIMITY);
                prox.setFieldname(node.getFieldname());
                prox.distance = node.getDistance();
                prox.ordered = node.isOrdered();

                for (int i=0; i<tokens.size(); i++) {
                    String token = tokens.get(i);

                    prox.jjtAddChild(Utils.convertToWildcardNode(node.getFieldname(), node.getOperator(), token), i);
                }

                return build(prox);
            }
        } else {
            // remove escapes
            for (int i=0; i<tokens.size(); i++) {
                tokens.set(i, Utils.unescape(tokens.get(i)));
            }

            // build proper filters
            if (tokens.size() == 1) {
                // only 1 token, so just return a term filter
                return buildStandard(node, new FBF() {
                    @Override
                    public FilterBuilder b(QueryParserNode n) {
                        return termFilter(n.getFieldname(), n.getValue());
                    }
                });
            } else {
                // more than 1 token, so return a query filter
                return buildStandard(node, new FBF() {
                    @Override
                    public FilterBuilder b(QueryParserNode n) {
                        return queryFilter(matchPhraseQuery(n.getFieldname(), Utils.join(tokens)));
                    }
                });
            }
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

        return spanTermQuery(prox.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNull node) {
        // when building spans, treat 'null' as a regular term
        return spanTermQuery(prox.getFieldname(), String.valueOf(node.getValue()));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTNotNull node) {
        return spanMultiTermQueryBuilder(wildcardQuery(prox.getFieldname(), String.valueOf(node.getValue())));
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
        return spanMultiTermQueryBuilder(prefixQuery(prox.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTWildcard node) {
        return spanMultiTermQueryBuilder(wildcardQuery(prox.getFieldname(), String.valueOf(node.getValue())));
    }

    private SpanQueryBuilder buildSpan(ASTProximity prox, ASTPhrase node) {
        if (prox.getOperator() == QueryParserNode.Operator.REGEX)
            return spanMultiTermQueryBuilder(regexpQuery(node.getFieldname(), node.getEscapedValue()));

        final List<String> tokens = Utils.tokenizePhrase(client, metadataManager, node.getFieldname(), node.getEscapedValue());
        for (int i=0; i<tokens.size(); i++) {
            String token = tokens.get(i);

            for (int j=0; j<WILDCARD_TOKENS.length; j++) {
                String wildcard = WILDCARD_TOKENS[j];
                String replacement = WILDCARD_VALUES[j];

                if (token.contains(wildcard)) {
                    tokens.set(i, token = token.replaceAll(wildcard, replacement));
                }
            }
        }

        for (Iterator<String> itr = tokens.iterator(); itr.hasNext();) {
            String token = itr.next();
            if (token.length() == 1) {
                if (token.equals("\\"))
                    itr.remove();
            }
        }

        if (tokens.size() == 1) {
            return buildSpan(prox, Utils.convertToWildcardNode(node.getFieldname(), node.getOperator(), tokens.get(0)));
        } else {
            SpanNearQueryBuilder qb = spanNearQuery();
            for (String token : tokens) {
                qb.clause(buildSpan(prox, Utils.convertToWildcardNode(node.getFieldname(), node.getOperator(), token)));
            }
            qb.slop(0);
            qb.inOrder(true);
            return qb;
        }
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
                throw new QueryRewriteException("Unexpected operator: " + node.getOperator());
        }
    }

    private FilterBuilder maybeNest(QueryParserNode node, FilterBuilder fb) {
        if (withDepth == 0 && node.isNested()) {
            return nestedFilter(node.getNestedPath(), fb).join(shouldJoinNestedFilter());
        } else if (!node.isNested()) {
            if (_isBuildingAggregate)
                return matchAllFilter();
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

    private FilterBuilder makeParentFilter(ASTExpansion node) {
        if (ignoreASTChild)
            return null;

        ASTIndexLink link = node.getIndexLink();
        IndexMetadata md = metadataManager.getMetadata(link);
        if (md != null && md.getNoXact())
            return null;

        QueryRewriter qr = new QueryRewriter(client, indexName, searchPreference, input, allowSingleIndex, true, true);
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
                throw new QueryRewriteException(response.getFailedShards() + " shards failed:\n" + sb);
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
            throw new QueryRewriteException(e);
        }
    }

    private FilterBuilder expand(final ASTExpansion root, final ASTIndexLink link) {
        if (isInTestMode())
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
                                throw new QueryRewriteException("Field equivalency cannot be determined");
                            }
                        } else {
                            // although I think we can with a while() loop that keeps resolving field data with each
                            // node in the path
                            throw new QueryRewriteException("Don't know how to resolve multiple levels of indirection");
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

    protected boolean isInTestMode() {
        return false;
    }
}
