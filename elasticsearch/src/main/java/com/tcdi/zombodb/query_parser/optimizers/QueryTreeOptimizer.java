/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2017 ZomboDB, LLC
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
import com.tcdi.zombodb.query_parser.metadata.FieldAndIndexPair;
import com.tcdi.zombodb.query_parser.metadata.IndexMetadataManager;

import java.util.*;

public class QueryTreeOptimizer {
    private final ASTQueryTree tree;
    private final IndexMetadataManager metadataManager;

    public QueryTreeOptimizer(ASTQueryTree tree, IndexMetadataManager metadataManager) {
        this.tree = tree;
        this.metadataManager = metadataManager;
    }

    public void optimize() {
        validateAndFixProximityChainFieldnames(tree);
        expandFieldLists(tree, tree.getFieldLists());
        expandAllField(tree, metadataManager.getMyIndex());
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

    private void expandAllField(QueryParserNode root, ASTIndexLink currentIndex) {
        if (root == null || !root.hasChildren())
            return;

        for (int i = 0, many = root.getChildren().size(); i < many; i++) {
            QueryParserNode child = root.getChild(i);

            if (child instanceof ASTIndexLink || child instanceof ASTAggregate || child instanceof ASTSuggest)
                continue;

            String fieldname = child.getFieldname();
            if (fieldname != null) {
                if ("_all".equals(fieldname)) {
                    ASTOr group = new ASTOr(QueryParserTreeConstants.JJTOR);
                    for (FieldAndIndexPair pair : metadataManager.resolveAllField()) {
                        ASTIndexLink link = pair.link != null ? pair.link : currentIndex;
                        QueryParserNode copy = child.copy();

                        copy.forceFieldname(pair.fieldname);
                        copy.setIndexLink(link);

                        group.jjtAddChild(copy, group.jjtGetNumChildren());
                    }

                    if (group.jjtGetNumChildren() == 1) {
                        root.replaceChild(child, group.jjtGetChild(0));
                    } else {
                        root.replaceChild(child, group);
                    }
                }
            }

            if (!(child instanceof ASTArray))
                expandAllField(child, child instanceof ASTExpansion ? metadataManager.findField(child.getIndexLink().getLeftFieldname()) : currentIndex);
        }
    }

    private void validateAndFixProximityChainFieldnames(QueryParserNode root) {
        if (root.getChildren() == null || root.getChildren().size() == 0)
            return;

        for (QueryParserNode child : root) {
            if (child instanceof ASTProximity) {
                {
                    Set<String> fieldnames = new HashSet<>();
                    for (QueryParserNode n : child) {
                        if (n.getFieldname() != null)
                            fieldnames.add(n.getFieldname());
                    }
                    if (fieldnames.size() > 1)
                        throw new RuntimeException("Cannot mix fieldnames in PROXIMITY expression");
                    else if (fieldnames.size() == 1)
                        child.setFieldname(fieldnames.iterator().next());
                }
            } else {
                validateAndFixProximityChainFieldnames(child);
            }
        }
    }

    static void rollupParentheticalGroups(QueryParserNode root) {
        if (root.getChildren() == null || root.getChildren().size() == 0)
            return;

        if (root instanceof ASTAnd || root instanceof ASTOr) {
            boolean isAnd = root instanceof ASTAnd;
            begin: while(true) {
                for (int i = 0, many = root.getChildren().size(); i < many; i++) {
                    QueryParserNode child = (QueryParserNode) root.getChild(i);

                    if ((isAnd && child instanceof ASTAnd) || (!isAnd && child instanceof ASTOr)) {
                        root.getChildren().remove(i);
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

        QueryParserNode parent = (QueryParserNode) root.jjtGetParent();
        for (int i = 0, many = root.getChildren().size(); i < many; i++) {
            if (parent.getChild(i) == root) {
                QueryParserNode child = (QueryParserNode) root.getChild(0);
                parent.jjtAddChild(child, i);
                child.jjtSetParent(parent);
            }
        }
    }

    private void mergeLiterals(QueryParserNode root) {
        if (root.getChildren() == null || root.getChildren().size() == 0)
            return;

        final boolean isAnd = root instanceof ASTAnd || root instanceof ASTWith;
        ASTArray array;

        Map<Integer, ASTArray> arraysByField = new TreeMap<>();
        boolean needsRenumber = false;
        for (int i = 0, many = root.getChildren().size(); i < many; i++) {
            QueryParserNode child = (QueryParserNode) root.getChild(i);
            if (child instanceof ASTAggregate)
                continue;

            if (child.isNested(metadataManager) && root instanceof ASTAnd)
                continue;

            if (child instanceof ASTWord || child instanceof ASTPhrase || child instanceof ASTNumber || child instanceof ASTBoolean || child instanceof ASTArray) {
                if (child.getOperator() == QueryParserNode.Operator.CONTAINS || child.getOperator() == QueryParserNode.Operator.EQ) {
                    if (child instanceof ASTArray && isAnd)
                        continue;   // arrays within an ASTAnd cannot be merged

                    if (child.getBoost() != root.getBoost())
                        continue;

                    array = null;
                    for (ASTArray a : arraysByField.values()) {
                        if (a.getFieldname().equals(child.getFieldname()) && a.getOperator() == child.getOperator() && (a.getIndexLink() == child.getIndexLink() || a.getIndexLink().equals(child.getIndexLink()))) {
                            array = a;
                            break;
                        }
                    }

                    if (array == null) {
                        array = new ASTArray(QueryParserTreeConstants.JJTARRAY);
                        array.setAnd(isAnd);
                        array.setFieldname(child.getFieldname());
                        array.setOperator(child.getOperator());
                        array.setIndexLink(child.getIndexLink());
                        arraysByField.put(i, array);
                    }

                    if (array.jjtGetParent() == null) {
                        if (child instanceof ASTArray) {
                            for (QueryParserNode elem : child) {
                                elem.setFieldname(child.getFieldname());
                                elem.setOperator(child.getOperator());
                                elem.setIndexLink(child.getIndexLink());
                                elem.setBoost(child.getBoost());
                            }
                            array.adoptChildren(child);
                        } else {
                            array.jjtAddChild(child, array.jjtGetNumChildren());
                            child.jjtSetParent(array);
                        }
                    }

                    root.removeNode(i);
                    needsRenumber = true;
                }
            }
        }

        if (needsRenumber) {
            for (Map.Entry<Integer, ASTArray> entry : arraysByField.entrySet()) {
                int idx = entry.getKey();
                ASTArray child = entry.getValue();

                if (child.jjtGetNumChildren() == 1) {
                    root.jjtAddChild(child.getChild(0), idx);
                    child.getChild(0).jjtSetParent(root);
                } else {
                    root.jjtAddChild(child, idx);
                    child.jjtSetParent(root);
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
        if (root.getChildren() == null || root.getChildren().size() == 0)
            return;

        Map<String, ASTArray> arraysByField = new TreeMap<>();
        Set<QueryParserNode> toRemove = new HashSet<>();
        for (int i = 0, many = root.getChildren().size(); i < many; i++) {
            QueryParserNode child = (QueryParserNode) root.getChild(i);
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
        // need to build the ASTOr structure from the bottom-up so that
        // nested generated expansions don't exclude rows from inner-expansions
        // where the join field value is null
        Stack<ASTExpansion> stack = ExpansionOptimizer.buildExpansionStack(root, new Stack<ASTExpansion>());

        while (!stack.empty()) {
            ASTExpansion expansion = stack.pop();
            QueryParserNode parent = (QueryParserNode) expansion.jjtGetParent();
            if (expansion.isGenerated()) {
                ASTOr or = new ASTOr(QueryParserTreeConstants.JJTOR);
                or.jjtAddChild(expansion, 0);
                QueryParserNode queryNode = expansion.getQuery().copy();
                or.jjtAddChild(queryNode, 1);
                parent.replaceChild(expansion, or);
            }
        }
    }
}
