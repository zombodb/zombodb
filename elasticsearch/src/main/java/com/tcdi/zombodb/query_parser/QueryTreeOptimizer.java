/*
 * Copyright 2013-2015 Technology Concepts & Design, Inc
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

        promoteNestedGroups(tree);
        int cnt;
        do {
            cnt = mergeAdjacentNestedGroups(tree);

            rollupParentheticalGroups(tree);
            mergeLiterals(tree);
            mergeArrays(tree);
        } while (cnt > 0);

        reduce(tree);
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

    private int mergeAdjacentNestedGroups(QueryParserNode root) {
        int total = 0;

        for (QueryParserNode child : root) {
            total += mergeAdjacentNestedGroups(child);
        }

        Map<String, List<ASTNestedGroup>> sameGroups = new HashMap<>();
        int cnt = 0;
        for (QueryParserNode child : root) {
            if (child instanceof ASTNestedGroup) {
                String base = child.getFieldname();
                List<ASTNestedGroup> groups = sameGroups.get(base);
                if (groups == null)
                    sameGroups.put(base, groups = new ArrayList<>());

                groups.add((ASTNestedGroup) child);
                cnt++;
            }
        }

        if (cnt > 1) {

            for (Map.Entry<String, List<ASTNestedGroup>> entry : sameGroups.entrySet()) {
                QueryParserNode container;
                if (root instanceof ASTAnd)
                    container = new ASTAnd(QueryParserTreeConstants.JJTAND);
                else if (root instanceof ASTOr)
                    container = new ASTOr(QueryParserTreeConstants.JJTOR);
                else if (root instanceof ASTNot)
                    container = new ASTAnd(QueryParserTreeConstants.JJTAND);
                else
                    throw new RuntimeException("Don't know about parent container type: " + root.getClass());

                ASTNestedGroup group = (ASTNestedGroup) entry.getValue().get(0).copy();
                group.children = null;
                group.jjtAddChild(container, 0);

                for (ASTNestedGroup existingGroup : entry.getValue()) {
                    container.adoptChildren(existingGroup);
                    root.removeNode(existingGroup);
                }

                root.renumber();
                root.jjtAddChild(group, root.jjtGetNumChildren());
                if (entry.getValue().size() > 1)
                    total++;
            }
        }

        return total;
    }

    private void promoteNestedGroups(ASTQueryTree root) {
        for (QueryParserNode child : root)
            promoteNestedGroups(child);
    }

    private void promoteNestedGroups(QueryParserNode root) {

        while (root instanceof ASTParent)
            root = root.getChild(0);
        while (root instanceof ASTChild)
            root = root.getChild(0);
        while (root instanceof ASTExpansion)
            root = ((ASTExpansion) root).getQuery();

        if (root == null || root instanceof ASTAggregate || root instanceof ASTSuggest || root instanceof ASTOptions)
            return;

        Set<String> groupFields = new HashSet<>();
        int count = collectNestedGroups(root, groupFields).size();
        String base = count > 0 ? groupFields.iterator().next() : null;

        if (count == 1 && base != null) {
            ASTNestedGroup group = new ASTNestedGroup(QueryParserTreeConstants.JJTNESTEDGROUP);
            group.setFieldname(base);

            ((QueryParserNode)root.parent).replaceChild(root, group);
            group.parent = root.parent;
            group.jjtAddChild(root, 0);
        } else {
            for (QueryParserNode child : root) {
                while (child instanceof ASTNot)
                    child = child.getChild(0);

                promoteNestedGroups(child);
            }
        }
    }

    private Set<String> collectNestedGroups(QueryParserNode root, Set<String> names) {
        for (QueryParserNode child : root)
            collectNestedGroups(child, names);

        if (root.getFieldname() != null) {
            int idx = root.getFieldname().indexOf('.');

            if (idx > -1)
                names.add(root.getFieldname().substring(0, idx));
            else
                names.add(null);
        }

        return names;
    }

    private void mergeLiterals(QueryParserNode root) {
        if (root.children == null || root.children.size() == 0)
            return;

        final boolean isAnd = root instanceof ASTAnd || (root instanceof ASTNestedGroup && ((ASTNestedGroup) root).isAnd());
        ASTArray array;

        Map<Integer, ASTArray> arraysByField = new TreeMap<>();
        Set<QueryParserNode> toRemove = new HashSet<>();
        for (int i=0, many=root.children.size(); i<many; i++) {
            QueryParserNode child = (QueryParserNode) root.children.get(i);
            if (child instanceof ASTAggregate)
                continue;

            if (child instanceof ASTWord || child instanceof ASTNumber || child instanceof ASTBoolean || child instanceof ASTArray) {
                if (child.getOperator() == QueryParserNode.Operator.CONTAINS || child.getOperator() == QueryParserNode.Operator.EQ || child.getOperator() == QueryParserNode.Operator.NE) {
                    if (child instanceof ASTArray && isAnd)
                        continue;   // arrays within an ASTAnd cannot be merged

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
            if ((child instanceof ASTAnd) || (child instanceof ASTOr) || (child instanceof ASTNot) || (child instanceof ASTNestedGroup) || (child instanceof ASTParent) || (child instanceof ASTChild) || (child instanceof ASTExpansion))
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
}
