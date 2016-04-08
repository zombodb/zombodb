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
package com.tcdi.zombodb.query_parser;

import java.util.*;

/**
 * Created by e_ridge on 12/23/14.
 */
public class QueryTreeOptimizer {
    private final ASTQueryTree tree;

    public QueryTreeOptimizer(ASTQueryTree tree) {
        this.tree = tree;
    }

    public void optimize() {
        expandFieldLists(tree, tree.getFieldLists());
        validateAndFixProximityChainFieldnames(tree);
        rollupParentheticalGroups(tree);
        mergeLiterals(tree);
        mergeArrays(tree);

        reduce(tree);
        convertGeneratedExpansionsToASTOr(tree);
    }

    private void expandFieldLists(QueryParserNode root, Map<String, ASTFieldListEntry> fieldLists) {
        if (fieldLists == null)
            return;

        for (QueryParserNode child : root) {
            if (child instanceof ASTFieldLists)
                continue;

            String fieldname = child.getFieldname();
            ASTFieldListEntry list = fieldLists.get(fieldname);

            if (list != null) {
                List<String> fields = list.getFields();

                if (fields.size() == 1) {
                    child.setFieldname(fields.get(0));
                } else {
                    ASTOr or = new ASTOr(QueryParserTreeConstants.JJTOR);
                    for (String fn : fields) {
                        QueryParserNode newNode = child.copy();
                        newNode.setFieldname(fn);
                        or.jjtAddChild(newNode, or.jjtGetNumChildren());

                        if (newNode instanceof ASTArray)
                            newNode.forceFieldname(fn);
                    }
                    root.replaceChild(child, or);
                }

            }

            if (child instanceof ASTArray)
                continue;

            // recurse into children
            expandFieldLists(child, fieldLists);
        }
    }

    void validateAndFixProximityChainFieldnames(QueryParserNode root) {
        if (root.children == null || root.children.size() == 0)
            return;

        for (QueryParserNode child : root) {
            if (child instanceof ASTProximity) {
                {
                    Set<String> fieldnames = new HashSet<String>();
                    for (QueryParserNode n : child) {
                        fieldnames.add(n.getFieldname());
                    }
                    if (fieldnames.size() > 1)
                        throw new RuntimeException("Cannot mix fieldnames in PROXIMITY expression");
                    child.setFieldname(fieldnames.iterator().next());
                }
            } else {
                validateAndFixProximityChainFieldnames(child);
            }
        }
    }

    static void rollupParentheticalGroups(QueryParserNode root) {
        if (root.children == null || root.children.size() == 0)
            return;

        if (root instanceof ASTAnd || root instanceof ASTOr) {
            boolean isAnd = root instanceof ASTAnd;
            begin: while(true) {
                for (int i = 0, many = root.children.size(); i < many; i++) {
                    QueryParserNode child = (QueryParserNode) root.children.get(i);

                    if ((isAnd && child instanceof ASTAnd) || (!isAnd && child instanceof ASTOr)) {
                        root.children.remove(i);
                        root.renumber();
                        root.adoptChildren(child);
                        continue begin;
                    }
                }
                break;
            }
        }

        // recursively optimize children the same way
        for (QueryParserNode child : root)
            rollupParentheticalGroups(child);
    }

    private void reduce(QueryParserNode root) {
        for (QueryParserNode child : root)
            reduce(child);

        if ((!(root instanceof ASTAnd) && !(root instanceof ASTOr)) || root.jjtGetNumChildren() > 1)
            return;

        QueryParserNode parent = (QueryParserNode) root.parent;
        for (int i=0, many=root.children.size(); i<many; i++) {
            if (parent.children.get(i) == root) {
                QueryParserNode child = (QueryParserNode) root.children.get(0);
                parent.children.put(i, child);
                child.jjtSetParent(parent);
            }
        }
    }

    private void mergeLiterals(QueryParserNode root) {
        if (root.children == null || root.children.size() == 0)
            return;

        final boolean isAnd = root instanceof ASTAnd || root instanceof ASTWith;
        ASTArray array;

        Map<Integer, ASTArray> arraysByField = new TreeMap<>();
        Set<QueryParserNode> toRemove = new HashSet<>();
        for (int i=0, many=root.children.size(); i<many; i++) {
            QueryParserNode child = (QueryParserNode) root.children.get(i);
            if (child instanceof ASTAggregate)
                continue;

            if (child.isNested() && root instanceof ASTAnd)
                continue;

            if (child instanceof ASTWord || child instanceof ASTNumber || child instanceof ASTBoolean || child instanceof ASTArray) {
                if (child.getOperator() == QueryParserNode.Operator.CONTAINS || child.getOperator() == QueryParserNode.Operator.EQ || child.getOperator() == QueryParserNode.Operator.NE) {
                    if (child instanceof ASTArray && isAnd)
                        continue;   // arrays within an ASTAnd cannot be merged

                    if (child.boost != root.boost)
                        continue;

                    if (child instanceof ASTArray || !Utils.isComplexTerm(child.getValue().toString())) {

                        array = null;
                        for (ASTArray a : arraysByField.values()) {
                            if (a.getFieldname().equals(child.getFieldname()) && a.getOperator() == child.getOperator()) {
                                array = a;
                                break;
                            }
                        }

                        if (array == null) {
                            array = new ASTArray(QueryParserTreeConstants.JJTARRAY);
                            array.setAnd(isAnd);
                            array.setFieldname(child.getFieldname());
                            array.setOperator(child.getOperator());
                            arraysByField.put(i, array);
                        }

                        if (array.parent == null) {
                            if (child instanceof ASTArray) {
                                array.adoptChildren(child);
                            } else {
                                array.jjtAddChild(child, array.jjtGetNumChildren());
                                child.parent = array;
                            }
                        }

                        toRemove.add(child);
                    }
                }
            }
        }

        if (!toRemove.isEmpty()) {
            for (QueryParserNode node : toRemove) {
                root.removeNode(node);
            }

            for (Map.Entry<Integer, ASTArray> entry : arraysByField.entrySet()) {
                int idx = entry.getKey();
                ASTArray child = entry.getValue();

                if (child.jjtGetNumChildren() == 1) {
                    root.jjtAddChild(child.getChild(0), idx);
                    child.getChild(0).parent = root;
                } else {
                    root.jjtAddChild(child, idx);
                    child.parent = root;
                }
            }
            root.renumber();
        }

        // recursively optimize children the same way
        for (QueryParserNode child : root)
            if ((child instanceof ASTWith) || (child instanceof ASTAnd) || (child instanceof ASTOr) || (child instanceof ASTNot) || (child instanceof ASTExpansion) || (child instanceof ASTFilter))
                mergeLiterals(child);
    }

    private void mergeArrays(QueryParserNode root) {
        if (root.children == null || root.children.size() == 0)
            return;

        Map<String, ASTArray> arraysByField = new TreeMap<>();
        Set<QueryParserNode> toRemove = new HashSet<>();
        for (int i=0, many=root.children.size(); i<many; i++) {
            QueryParserNode child = (QueryParserNode) root.children.get(i);
            if (child instanceof ASTAggregate)
                continue;

            if (child instanceof ASTArray) {
                ASTArray existing = arraysByField.get(child.getFieldname() + ((ASTArray) child).isAnd());

                if (existing == null) {
                    arraysByField.put(child.getFieldname() + ((ASTArray) child).isAnd(), (ASTArray) child);
                    continue;
                }

                existing.adoptChildren(child);
                toRemove.add(child);
            }
        }

        if (!toRemove.isEmpty()) {
            for (QueryParserNode node : toRemove) {
                root.removeNode(node);
            }
            root.renumber();
        }

        // recursively optimize children the same way
        for (QueryParserNode child : root)
            mergeArrays(child);
    }

    private void convertGeneratedExpansionsToASTOr(QueryParserNode root) {
        if (root.children == null || root.children.size() == 0)
            return;

        for (QueryParserNode child : root) {
            if (child instanceof ASTExpansion) {
                if (((ASTExpansion) child).isGenerated()) {
                    ASTOr or = new ASTOr(QueryParserTreeConstants.JJTOR);
                    or.jjtAddChild(child, 0);
                    QueryParserNode queryNode = ((ASTExpansion) child).getQuery().copy();
                    or.jjtAddChild(queryNode, 1);
                    root.replaceChild(child, or);
                }
            }

            convertGeneratedExpansionsToASTOr(child);
        }
    }
}
