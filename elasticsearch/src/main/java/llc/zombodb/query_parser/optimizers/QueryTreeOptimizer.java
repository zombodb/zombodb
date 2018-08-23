/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2018 ZomboDB, LLC
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
package llc.zombodb.query_parser.optimizers;

import llc.zombodb.query_parser.*;
import llc.zombodb.query_parser.metadata.FieldAndIndexPair;
import llc.zombodb.query_parser.metadata.IndexMetadataManager;

import java.util.*;

public class QueryTreeOptimizer {
    private final ASTQueryTree tree;
    private final IndexMetadataManager metadataManager;

    public QueryTreeOptimizer(ASTQueryTree tree, IndexMetadataManager metadataManager) {
        this.tree = tree;
        this.metadataManager = metadataManager;
    }

    public void optimize() {
        // NOTE:  pullOutOptionNodes() happens in QueryParser#parse()
        pullOutNodesOfType(tree, ASTLimit.class, true);
        pullOutNodesOfType(tree, ASTVisibility.class, true);
        pullOutNodesOfType(tree, ASTAggregate.class, false);
        pullOutNodesOfType(tree, ASTSuggest.class, true);
        pullOutNodesOfType(tree, ASTOptions.class, true);
        pullOutNodesOfType(tree, ASTFieldLists.class, true);
        validateWithOperators(tree);
        reduce(tree);
        validateAndFixProximityChainFieldnames(tree);
        expandFieldLists(tree, tree.getFieldLists());
        expandAllField(tree, metadataManager.getMyIndex());
        pullOutNullsFromArrays(tree);
        rollupParentheticalGroups(tree);
        mergeLiterals(tree);
        mergeArrays(tree);

        reduce(tree);
        convertGeneratedExpansionsToASTOr(tree);

        reduceWiths(tree);
        sortWithOperators(tree);
        generateWithNodesByPath(tree);
    }

    private void pullOutNodesOfType(ASTQueryTree tree, Class type, boolean recurse) {
        Collection<QueryParserNode> nodes = tree.getChildrenOfType(type, recurse);

        for (QueryParserNode node : nodes) {
            QueryParserNode parent = (QueryParserNode) node.jjtGetParent();

            parent.removeNode(node);
            parent.renumber();

            tree.jjtInsertChild(node, 0);
        }
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
            begin:
            while (true) {
                for (int i = 0, many = root.getChildren().size(); i < many; i++) {
                    QueryParserNode child = root.getChild(i);

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

    static void reduce(QueryParserNode root) {
        for (QueryParserNode child : root)
            reduce(child);

        if ((!(root instanceof ASTAnd) && !(root instanceof ASTOr)) || root.jjtGetNumChildren() > 1)
            return;

        QueryParserNode parent = (QueryParserNode) root.jjtGetParent();
        switch (root.jjtGetNumChildren()) {
            case 0:
                parent.removeNode(root);
                break;

            case 1:
                parent.replaceChild(root, root.getChildren().values().iterator().next());
                break;
        }

        parent.renumber();
    }

    static void reduceWiths(QueryParserNode root) {
        for (QueryParserNode child : root)
            reduceWiths(child);

        if (root instanceof ASTWith) {
            Node parent = root.jjtGetParent();
            if (parent instanceof ASTWith) {
                ((ASTWith) parent).removeNode(root);
                ((ASTWith) parent).renumber();
                ((ASTWith) parent).adoptChildren(root);
            }
        }
    }

    private void pullOutNullsFromArrays(QueryParserNode root) {
        if (root == null)
            return;

        boolean didNullPullUp = false;
        for (QueryParserNode child : root) {
            if (child instanceof ASTNull && root instanceof ASTArray) {
                QueryParserNode parent = (QueryParserNode) root.jjtGetParent();

                root.removeNode(child);
                root.renumber();

                if (!didNullPullUp) {
                    if (((ASTArray) root).isAnd()) {
                        ASTAnd and = new ASTAnd(QueryParserTreeConstants.JJTAND);
                        and.jjtAddChild(child, 0);
                        and.jjtAddChild(root, 1);
                        parent.replaceChild(root, and);
                        didNullPullUp = true;
                    } else {
                        ASTOr or = new ASTOr(QueryParserTreeConstants.JJTOR);
                        or.jjtAddChild(child, 0);
                        or.jjtAddChild(root, 1);
                        parent.replaceChild(root, or);
                        didNullPullUp = true;
                    }
                }

            } else {
                pullOutNullsFromArrays(child);
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
            QueryParserNode child = root.getChild(i);
            if (child instanceof ASTAggregate)
                continue;

            if (child.isNested(metadataManager) && root instanceof ASTAnd)
                continue;

            if (child instanceof ASTWord || child instanceof ASTPhrase || child instanceof ASTNumber || child instanceof ASTBoolean || child instanceof ASTArray) {
                if (child.getOperator() == QueryParserNode.Operator.CONTAINS || child.getOperator() == QueryParserNode.Operator.EQ || child.getOperator() == QueryParserNode.Operator.NE) {
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
                        array.setAnd( (isAnd && child.getOperator() != QueryParserNode.Operator.NE) || (!isAnd && child.getOperator() == QueryParserNode.Operator.NE) );
                        array.setFieldname(child.getFieldname());
                        array.setOperator(child.getOperator());
                        array.setIndexLink(child.getIndexLink());
                        arraysByField.put(i, array);
                    }

                    if (array.jjtGetParent() == null) {
                        if (child instanceof ASTArray) {
                            array.setAnd(((ASTArray) child).isAnd());
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
            QueryParserNode child = root.getChild(i);
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
        Stack<ASTExpansion> stack = buildExpansionStack(root, new Stack<ASTExpansion>());

        while (!stack.empty()) {
            ASTExpansion expansion = stack.pop();

            if (expansion.isSubselect())
                continue;
            else if (!expansion.isGenerated())
                continue;

            QueryParserNode parent = (QueryParserNode) expansion.jjtGetParent();
            ASTOr or = new ASTOr(QueryParserTreeConstants.JJTOR);
            or.jjtAddChild(expansion, 0);
            QueryParserNode queryNode = expansion.getQuery().copy();
            or.jjtAddChild(queryNode, 1);
            parent.replaceChild(expansion, or);
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

    private void validateWithOperators(ASTQueryTree tree) {
        for (ASTWith child : tree.getChildrenOfType(ASTWith.class)) {
            // this will throw if the base paths don't match
            child.validateNestedPaths();
        }
    }

    private void sortWithOperators(QueryParserNode root) {
        List<? extends QueryParserNode> nodes = root.getChildrenOfType(ASTWith.class);

        if (root instanceof ASTQueryTree && nodes.isEmpty())
            return;

        if (nodes.isEmpty() && root.getChildren() != null) {
            List<Node> children = new ArrayList<>(root.getChildren().values());
            List<QueryParserNode> resort = new ArrayList<>();

            sortChildrenByNestedPath(children, resort);

            // reassign the children in order of ascending path length
            root.getChildren().clear();
            int i = 0;
            for (Node node : children)
                root.jjtAddChild(node, i++);

            for (QueryParserNode node : resort)
                sortWithOperators(node);

        } else {
            for (QueryParserNode with : nodes) {
                List<Node> children = new ArrayList<>((with.getChildren().values()));
                List<QueryParserNode> resort = new ArrayList<>();

                sortChildrenByNestedPath(children, resort);

                // reassign the children in order of ascending path length
                with.getChildren().clear();
                int i = 0;
                for (Node node : children)
                    with.jjtAddChild(node, i++);

                for (QueryParserNode node : resort)
                    sortWithOperators(node);

            }
        }
    }

    private void sortChildrenByNestedPath(List<Node> children, List<QueryParserNode> resort) {
        children.sort((o1, o2) -> {
            QueryParserNode a = (QueryParserNode) o1;
            QueryParserNode b = (QueryParserNode) o2;
            String aPath = a.getNestedPath();
            String bPath = b.getNestedPath();

            if (aPath == null)
                resort.add(a);
            if (bPath == null)
                resort.add(b);

            if (aPath == null || bPath == null)
                return 0;

            return aPath.compareTo(bPath);
        });
    }

    private void generateWithNodesByPath(ASTQueryTree tree) {
        for (ASTWith with : tree.getChildrenOfType(ASTWith.class)) {
            generateWithNodesByPath(tree, with, with);
        }
    }

    private void generateWithNodesByPath(ASTQueryTree tree, ASTWith with, QueryParserNode root) {
        String currentPath = null;
        ASTWith currentWith = with;
        for (Node node : new ArrayList<>(root.getChildren().values())) {
            QueryParserNode child = (QueryParserNode) node;

            if (child.getNestedPath() == null) {
                continue;
            } else {

                if (currentPath == null) {
                    currentPath = child.getNestedPath();
                } else if (!child.getNestedPath().equals(currentPath)) {
//                    if (currentWith == with) {
                        currentWith = new ASTWith(QueryParserTreeConstants.JJTWITH);
                        root.replaceChild(child, currentWith);
                        currentPath = child.getNestedPath();
//                    }

                    currentWith.jjtAddChild(child, currentWith.jjtGetNumChildren());
                } else if (currentWith != with) {
                    currentWith.jjtAddChild(child, currentWith.jjtGetNumChildren());
                    root.removeNode(child);
                    root.renumber();
                }
            }
        }
    }
}
