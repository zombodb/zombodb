/*
 * Copyright 2017 ZomboDB, LLC
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
package com.tcdi.zombodb.query_parser.optimizers;

import com.tcdi.zombodb.query_parser.*;
import com.tcdi.zombodb.query_parser.metadata.IndexMetadata;
import com.tcdi.zombodb.query_parser.metadata.IndexMetadataManager;
import com.tcdi.zombodb.query_parser.rewriters.QueryRewriter;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.*;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class ExpansionOptimizer {
    private final QueryRewriter rewriter;
    private final ASTQueryTree tree;
    private final IndexMetadataManager metadataManager;
    private final Client client;
    private final String searchPreference;
    private final boolean doFullFieldDataLookup;

    private Stack<ASTExpansion> generatedExpansionsStack = new Stack<>();

    public ExpansionOptimizer(QueryRewriter rewriter, ASTQueryTree tree, IndexMetadataManager metadataManager, Client client, String searchPreference, boolean doFullFieldDataLookup) {
        this.rewriter = rewriter;
        this.tree = tree;
        this.metadataManager = metadataManager;
        this.client = client;
        this.searchPreference = searchPreference;
        this.doFullFieldDataLookup = doFullFieldDataLookup;
    }

    public void optimize() {
        outer: while(true) {
            Collection<ASTExpansion> expansions = tree.getChildrenOfType(ASTExpansion.class);

            for (ASTExpansion expansion : expansions) {
                if (expansion.jjtGetParent() == null)
                    continue outer;
                if (expansion.isGenerated())
                    generatedExpansionsStack.push(expansion);
                try {
                    expand(expansion);
                } finally {
                    if (expansion.isGenerated())
                        generatedExpansionsStack.pop();
                }
            }

            break;
        }

        mergeAdjacentANDs(tree);
        mergeAdjacentORs(tree);
        pullUpNOTs(tree);

        flatten();
    }

    private void flatten() {
        QueryParserNode queryNode = tree.getQueryNode();
        if (queryNode instanceof ASTAnd || queryNode instanceof ASTOr) {
            if (queryNode.jjtGetNumChildren() == 1)
                ((QueryParserNode) queryNode.jjtGetParent()).replaceChild(queryNode, queryNode.getChild(0));
        }
    }

    private void expand(final ASTExpansion root) {
        outer: while(true) {
            Stack<ASTExpansion> stack = buildExpansionStack(root, new Stack<>());

            ASTIndexLink myIndex = metadataManager.getMyIndex();
            QueryParserNode last;

            try {
                while (!stack.isEmpty()) {
                    ASTExpansion expansion = stack.pop();
                    if (expansion.jjtGetParent() == null)
                        break;

                    if (generatedExpansionsStack.isEmpty() && expansion.getIndexLink() == myIndex) {
                        last = expansion.getQuery();
                    } else {
                        if ("(null)".equals(expansion.getIndexLink().getLeftFieldname()))
                            last = expansion.getQuery();
                        else
                            last = loadFielddata(expansion, expansion.getIndexLink().getLeftFieldname(), expansion.getIndexLink().getRightFieldname());
                    }

                    // replace the ASTExpansion in the tree with the fieldData version
                    ((QueryParserNode) expansion.jjtGetParent()).replaceChild(expansion, last);
                    expansion.jjtSetParent(null);

                    if (!(last instanceof ASTArray)) {
                        continue outer;
                    }
                }
            } finally {
                metadataManager.setMyIndex(myIndex);
            }

            break;
        }
    }

    static Stack<ASTExpansion> buildExpansionStack(QueryParserNode root, Stack<ASTExpansion> stack) {

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

        QueryBuilder query = rewriter.applyVisibility(rewriter.build(nodeQuery));

        SearchResponse response = SearchAction.INSTANCE.newRequestBuilder(client)
                .setIndices(link.getIndexName())
                .setTypes("data")
                .setSize(0)
                .setPreference(searchPreference)
                .setRequestCache(true)
                .setQuery(query)
                .addAggregation(
                        terms(rightFieldname)
                                .field(rightFieldname)
                                .shardSize(!doFullFieldDataLookup ? 1024 : Integer.MAX_VALUE)
                                .size(!doFullFieldDataLookup ? 1024 : Integer.MAX_VALUE)
                )
                .get();

        Terms agg = (Terms) response.getAggregations().iterator().next();
        ASTArray array = new ASTArray(QueryParserTreeConstants.JJTARRAY);
        array.setFieldname(leftFieldname);
        array.setOperator(QueryParserNode.Operator.EQ);
        array.setExternalValues(() -> {
            final Iterator<? extends Terms.Bucket> buckets = agg.getBuckets().iterator();
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return buckets.hasNext();
                }

                @Override
                public Object next() {
                    return buckets.next().getKey();
                }

                @Override
                public void remove() {
                    buckets.remove();
                }
            };
        }, agg.getBuckets().size());

        QueryParserNode filterQuery = node.getFilterQuery();
        if (filterQuery != null) {
            ASTAnd and = new ASTAnd(QueryParserTreeConstants.JJTAND);
            and.jjtAddChild(array, 0);
            and.jjtAddChild(filterQuery, 1);
            return and;
        } else {
            return array;
        }
    }

    private void mergeAdjacentANDs(QueryParserNode root) {
        if (root instanceof ASTAnd) {
            if (root.jjtGetNumChildren() > 1) {

                Map<String, Map<QueryParserNode, Set<Object>>> terms = new HashMap<>();

                buildNodeSets(root, terms);

                for (Map<QueryParserNode, Set<Object>> map : terms.values()) {
                    if (map.size() > 1) {
                        Set<Object> merged = null;
                        QueryParserNode first = null;
                        for (Map.Entry<QueryParserNode, Set<Object>> entry : map.entrySet()) {
                            QueryParserNode node = entry.getKey();
                            Set<Object> value = entry.getValue();

                            if (merged == null) {
                                merged = value;
                                first = node;
                            } else {
                                merged.retainAll(value);
                            }
                            root.removeNode(node);
                        }
                        assert (first != null);

                        ASTArray array = new ASTArray(QueryParserTreeConstants.JJTARRAY);
                        array.setFieldname(first.getFieldname());
                        array.setOperator(first.getOperator());
                        array.setExternalValues(merged, merged.size());

                        root.renumber();
                        root.jjtAddChild(array, root.jjtGetNumChildren());
                    }
                }

            }
        } else {
            for (QueryParserNode child : root)
                mergeAdjacentANDs(child);
        }
    }

    private void mergeAdjacentORs(QueryParserNode root) {
        if (root instanceof ASTOr) {
            if (root.jjtGetNumChildren() > 1) {

                Map<String, Map<QueryParserNode, Set<Object>>> terms = new HashMap<>();

                buildNodeSets(root, terms);

                for (Map<QueryParserNode, Set<Object>> map : terms.values()) {
                    if (map.size() > 1) {
                        Set<Object> merged = null;
                        QueryParserNode first = null;
                        for (Map.Entry<QueryParserNode, Set<Object>> entry : map.entrySet()) {
                            QueryParserNode node = entry.getKey();
                            Set<Object> value = entry.getValue();

                            if (merged == null) {
                                merged = value;
                                first = node;
                            } else {
                                merged.addAll(value);
                            }
                            root.removeNode(node);
                        }
                        assert (first != null);

                        ASTArray array = new ASTArray(QueryParserTreeConstants.JJTARRAY);
                        array.setFieldname(first.getFieldname());
                        array.setOperator(first.getOperator());
                        array.setExternalValues(merged, merged.size());

                        root.renumber();
                        root.jjtAddChild(array, root.jjtGetNumChildren());
                    }
                }

            }
        } else {
            for (QueryParserNode child : root)
                mergeAdjacentORs(child);
        }
    }

    private void pullUpNOTs(QueryParserNode root) {
        if (root instanceof ASTAnd) {
            Collection<ASTNot> nots = root.getChildrenOfType(ASTNot.class);

            if (nots.size() > 0) {
                Map<String, Map<QueryParserNode, Set<Object>>> terms = new HashMap<>();

                buildNodeSets(root, terms);

                for (ASTNot not : nots) {
                    QueryParserNode child = not.getChild(0);
                    if (child instanceof ASTArray) {
                        final ASTArray notArray = (ASTArray) child;
                        if (notArray.hasExternalValues()) {
                            String fieldname = child.getFieldname();
                            Map<QueryParserNode, Set<Object>> array = terms.get(fieldname);

                            if (array != null) {
                                for (Map.Entry<QueryParserNode, Set<Object>> entry : array.entrySet()) {
                                    QueryParserNode node = entry.getKey();
                                    if (node instanceof ASTArray) {
                                        Set<Object> values = entry.getValue();

                                        values.removeAll(new HashSet<>(new AbstractCollection<Object>() {
                                            @Override
                                            public Iterator<Object> iterator() {
                                                return notArray.getExternalValues().iterator();
                                            }

                                            @Override
                                            public int size() {
                                                return notArray.getTotalExternalValues();
                                            }
                                        }));

                                        ((ASTArray) node).setExternalValues(values, values.size());
                                        ((QueryParserNode) notArray.jjtGetParent()).removeNode(notArray);
                                        ((QueryParserNode) notArray.jjtGetParent()).renumber();
                                    }
                                }
                            }
                        }
                    }

                    if (not.jjtGetNumChildren() == 0) {
                        ((QueryParserNode) not.jjtGetParent()).removeNode(not);
                        ((QueryParserNode) not.jjtGetParent()).renumber();
                    }
                }
            }
        }

        for (QueryParserNode child : root)
            pullUpNOTs(child);
    }

    private void buildNodeSets(QueryParserNode root, Map<String, Map<QueryParserNode, Set<Object>>> terms) {
        for (QueryParserNode child : root) {
            Set<Object> set = new HashSet<>();

            IndexMetadata md = metadataManager.getMetadataForField(child.getFieldname());
            String pkey = md.getPrimaryKeyFieldName();
            if (pkey == null || !pkey.equals(child.getFieldname()))
                continue;   // can only do this for primary key fields

            if (!(child.getOperator() == QueryParserNode.Operator.CONTAINS || child.getOperator() == QueryParserNode.Operator.EQ))
                continue;

            boolean didWork = false;
            if (child instanceof ASTArray) {
                ASTArray array = (ASTArray) child;

                if (!array.isAnd()) {
                    Iterable<Object> itr = array.hasExternalValues() ? array.getExternalValues() : array.getChildValues();
                    for (Object obj : itr) {
                        set.add(String.valueOf(obj));
                    }
                    didWork = true;
                }
            } else if (child instanceof ASTNumber || child instanceof ASTWord) {
                set.add(String.valueOf(child.getValue()));
                didWork = true;
            }

            if (didWork) {
                Map<QueryParserNode, Set<Object>> map = terms.computeIfAbsent(child.getFieldname(), k -> new HashMap<>());

                map.put(child, set);
            }
        }
    }
}
