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

import java.util.*;

/**
 * Created by e_ridge on 4/21/15.
 */
public class IndexLinkOptimizer {
    private final ASTQueryTree tree;
    private final IndexMetadataManager metadataManager;

    private Set<ASTIndexLink> usedIndexes = new HashSet<>();

    public IndexLinkOptimizer(ASTQueryTree tree, IndexMetadataManager metadataManager) {
        this.tree = tree;
        this.metadataManager = metadataManager;
    }

    public void optimize() {
        try {
            fixNestedGroups(tree);

            expand_allFieldAndAssignIndexLinks(tree, metadataManager.getMyIndex());

            QueryTreeOptimizer.rollupParentheticalGroups(tree);

            injectASTExpansionNodes(tree);

            int before;
            do {
                before = tree.countNodes();
                while (mergeAdjacentExpansions(tree) > 0) ;

                QueryTreeOptimizer.rollupParentheticalGroups(tree);
            } while (tree.countNodes() != before);


            rewriteIndirectReferenceIndexLinks(tree);

            ASTAggregate agg = tree.getAggregate();
            while (agg != null) {
                usedIndexes.add(metadataManager.findField(agg.getFieldname()));
                agg = agg.getSubAggregate();
            }

            metadataManager.setUsedIndexes(usedIndexes);
        } catch (CloneNotSupportedException e) {
            // should never happen
            throw new RuntimeException(e);
        }
    }


    private void expand_allFieldAndAssignIndexLinks(QueryParserNode root, ASTIndexLink currentIndex) throws CloneNotSupportedException {
        if (root == null || root.children == null || root.children.isEmpty() || (root instanceof ASTExpansion && !((ASTExpansion) root).isGenerated())) {
            if (root instanceof ASTExpansion)
                usedIndexes.add(metadataManager.getIndexLinkByIndexName(root.getIndexLink().getIndexName()));
            return;
        }

        if (root instanceof  ASTExpansion && ((ASTExpansion) root).isGenerated()) {
            ASTIndexLink left = metadataManager.findField(root.getIndexLink().getLeftFieldname());
            ASTIndexLink right = metadataManager.findField(root.getIndexLink().getRightFieldname());
            usedIndexes.add(left);
            usedIndexes.add(right);
        }

        for (int i = 0, many = root.children.size(); i < many; i++) {
            QueryParserNode child = (QueryParserNode) root.children.get(i);
            String fieldname = child.getFieldname();

            if (child instanceof ASTIndexLink || child instanceof ASTParent || child instanceof ASTAggregate || child instanceof ASTSuggest)
                continue;

            if (fieldname != null && !(child instanceof ASTExpansion) && !(child instanceof ASTNotNested)) {
                if ("_all".equals(fieldname)) {
                    ASTOr group = new ASTOr(QueryParserTreeConstants.JJTOR);
                    for (FieldAndIndexPair pair : metadataManager.resolveAllField()) {
                        ASTIndexLink link = pair.link != null ? pair.link : currentIndex;
                        QueryParserNode copy = child.copy();

                        copy.forceFieldname(pair.fieldname);
                        copy.setIndexLink(link);

                        group.jjtAddChild(copy, group.jjtGetNumChildren());
                        usedIndexes.add(link);
                    }

                    group.parent = root;
                    if (group.jjtGetNumChildren() == 1) {
                        root.jjtAddChild(group.jjtGetChild(0), i);
                        group.jjtGetChild(0).jjtSetParent(root);
                    } else {
                        root.replaceChild(child, group);
                        root.jjtAddChild(group, i);
                    }
                } else if (!fieldname.startsWith("_")) {
                    ASTIndexLink link = metadataManager.findField(fieldname);
                    child.setIndexLink(link);
                    usedIndexes.add(link);
                }
            }

            if (!(child instanceof ASTArray))
                expand_allFieldAndAssignIndexLinks(child, child instanceof ASTExpansion ? metadataManager.findField(child.getIndexLink().getLeftFieldname()) : currentIndex);
        }
    }

    private void fixNestedGroups(QueryParserNode root) {
        Collection<QueryParserNode> children = root.getChildren();

        if (root instanceof ASTNestedGroup) {
            if (root.getFieldname() != null) {
                for (IndexMetadataManager.IndexLinkAndMapping ilam : metadataManager.getAllMappings()) {
                    if (root.getNestedPath().equalsIgnoreCase(ilam.link.getFieldname())) {
                        // it's not actually a nested group
                        // so move children up and drop this node entirely
                        ((QueryParserNode) root.parent).removeNode(root);
                        ((QueryParserNode) root.parent).renumber();
                        ((QueryParserNode) root.parent).adoptChildren(root);
//                        stripPath(root, root.getNestedPath());
                    }
                }
            }
        }

        for (QueryParserNode child : children)
            fixNestedGroups(child);
    }

    private void injectASTExpansionNodes(ASTQueryTree tree) {
        ASTChild childQuery = (ASTChild) tree.getChild(ASTChild.class);
        if (childQuery != null) {
            injectASTExpansionNodes(childQuery);
        } else {
            for (QueryParserNode child : tree) {
                if (child instanceof ASTOptions || child instanceof ASTAggregate || child instanceof ASTSuggest)
                    continue;
                injectASTExpansionNodes(child);
            }
        }
    }

    private void injectASTExpansionNodes(QueryParserNode root) {
        if (root instanceof ASTParent)
            return;

        while (root instanceof ASTChild)
            root = root.getChild(0);
        while (root instanceof ASTExpansion)
            root = ((ASTExpansion) root).getQuery();

        Set<ASTIndexLink> links = collectIndexLinks(root, new HashSet<ASTIndexLink>());
        if (links.size() == 0)
            return;

        if (links.size() == 1) {
            ASTIndexLink link = links.iterator().next();
            if (link == null)
                return;

            ASTExpansion expansion = new ASTExpansion(QueryParserTreeConstants.JJTEXPANSION);
            expansion.jjtAddChild(link, 0);

            ((QueryParserNode)root.parent).replaceChild(root, expansion);
            expansion.jjtAddChild(root, 1);

        } else {
            for (QueryParserNode child : root)
                injectASTExpansionNodes(child);
        }

    }

    private Set<ASTIndexLink> collectIndexLinks(QueryParserNode root, Set<ASTIndexLink> links) {
        if (root == null)
            return links;

        if (root instanceof ASTParent) {
            // so that ASTParent nodes never get grouped in an ASTExpansion node
            links.add(null);
            return links;
        }

        for (QueryParserNode child : root)
            collectIndexLinks(child, links);

        if (root.getIndexLink() != null)
            links.add(root.getIndexLink());
        return links;
    }

    private int mergeAdjacentExpansions(QueryParserNode root) {
        int total = 0;

        for (QueryParserNode child : root) {
            total += mergeAdjacentExpansions(child);
        }

        Map<ASTIndexLink, List<ASTExpansion>> sameExpansions = new IdentityHashMap<>();
        int cnt = 0;
        for (QueryParserNode child : root) {
            if (child instanceof ASTExpansion) {
                ASTIndexLink key = child.getIndexLink();
                List<ASTExpansion> groups = sameExpansions.get(key);
                if (groups == null)
                    sameExpansions.put(key, groups = new ArrayList<>());

                groups.add((ASTExpansion) child);
                cnt++;
            }
        }

        if (cnt > 1) {

            for (Map.Entry<ASTIndexLink, List<ASTExpansion>> entry : sameExpansions.entrySet()) {
                if (entry.getValue().size() > 1) {
                    QueryParserNode container;
                    if (root instanceof ASTAnd)
                        container = new ASTAnd(QueryParserTreeConstants.JJTAND);
                    else if (root instanceof ASTOr)
                        container = new ASTOr(QueryParserTreeConstants.JJTOR);
                    else if (root instanceof ASTNot)
                        container = new ASTAnd(QueryParserTreeConstants.JJTAND);
                    else
                        throw new RuntimeException("Don't know about parent container type: " + root.getClass());

                    ASTExpansion newExpansion = new ASTExpansion(QueryParserTreeConstants.JJTEXPANSION);
                    newExpansion.jjtAddChild(entry.getValue().get(0).getIndexLink(), 0);
                    newExpansion.setGenerated(entry.getValue().get(0).isGenerated());
                    newExpansion.jjtAddChild(container, 1);

                    int idx = 0;
                    for (ASTExpansion existingExpansion : entry.getValue()) {
                        container.jjtAddChild(existingExpansion.getQuery(), container.jjtGetNumChildren());

                        if (idx == 0)
                            root.replaceChild(existingExpansion, newExpansion);
                        else
                            root.removeNode(existingExpansion);
                        idx++;
                    }

                    root.renumber();
                    if (entry.getValue().size() > 1)
                        total++;
                }
            }
        }

        return total;
    }

    private void rewriteIndirectReferenceIndexLinks(QueryParserNode node) {
        if (node instanceof ASTExpansion) {
            if (((ASTExpansion) node).isGenerated()) {
                final ASTIndexLink link = node.getIndexLink();
                if (link.getIndexName().equals("this.index")) {
                    ASTIndexLink newLink = new ASTIndexLink(QueryParserTreeConstants.JJTINDEXLINK) {
                        @Override
                        public String getLeftFieldname() {
                            return link.getLeftFieldname();
                        }

                        @Override
                        public String getIndexName() {
                            return metadataManager.findField(link.getLeftFieldname()).getIndexName();
                        }

                        @Override
                        public String getRightFieldname() {
                            return link.getRightFieldname();
                        }
                    };
                    node.jjtAddChild(newLink, 0);
                }
            }
        }

        for (QueryParserNode child : node)
            rewriteIndirectReferenceIndexLinks(child);
    }

    static void stripPath(QueryParserNode root, String path) {
        if (root.getFieldname() != null && root.getFieldname().startsWith(path+"."))
            root.setFieldname(root.getFieldname().substring(path.length()+1));

        for (QueryParserNode child : root) {
            stripPath(child, path);
        }

    }

}
