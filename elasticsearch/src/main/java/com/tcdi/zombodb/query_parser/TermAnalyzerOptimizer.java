/*
 * Copyright 2016 ZomboDB, LLC
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

import org.elasticsearch.client.Client;

import java.util.ArrayList;
import java.util.List;

public class TermAnalyzerOptimizer {

    private final Client client;
    private final IndexMetadataManager metadataManager;
    private final ASTQueryTree tree;

    public TermAnalyzerOptimizer(Client client, IndexMetadataManager metadataManager, ASTQueryTree tree) {
        this.client = client;
        this.metadataManager = metadataManager;
        this.tree = tree;
    }

    public void optimize() {
        analyzeNodes(tree);
        pullOutComplexTokensFromArrays(tree);
    }

    private boolean analyzeNodes(QueryParserNode root) {
        boolean changed = false;

        if (root == null || root instanceof ASTOptions || root instanceof ASTFieldLists || root instanceof ASTAggregate)
            return false;

        if (root.isStringValue() && (root instanceof ASTWord || root instanceof ASTPhrase || root instanceof ASTFuzzy || root instanceof ASTPrefix || root instanceof ASTWildcard))
            changed = analyzeToken(root);


        start_over: while(true) {
            for (QueryParserNode child : root) {
                if (analyzeNodes(child)) {
                    continue start_over;
                }
            }
            break;
        }

        return changed;
    }

    private boolean analyzeToken(QueryParserNode node) {
        switch (node.getOperator()) {
            case CONCEPT:
            case FUZZY_CONCEPT:
            case REGEX:
                return false;
        }

        QueryParserNode parentNode = (QueryParserNode) node.parent;
        QueryParserNode newNode = Utils.rewriteToken(client, metadataManager, node);
        if (newNode instanceof ASTWord && "".equals(newNode.getValue())) {
            parentNode.removeNode(node);
            parentNode.renumber();
            return true;
        } else if (newNode != node) {
            parentNode.replaceChild(node, newNode);
            return false;
        } else {
            return false;
        }
    }

    private void pullOutComplexTokensFromArrays(QueryParserNode root) {
        if (root instanceof ASTArray) {
            List<QueryParserNode> complex = new ArrayList<>();
            for (QueryParserNode child : root) {
                if (child instanceof ASTWord || child instanceof ASTNumber || child instanceof ASTBoolean)
                    continue;
                complex.add(child);
                root.removeNode(child);
            }
            if (!complex.isEmpty()) {
                root.renumber();
                ASTOr or = new ASTOr(QueryParserTreeConstants.JJTOR);
                for (QueryParserNode node : complex)
                    or.jjtAddChild(node, or.jjtGetNumChildren());
                ((QueryParserNode) root.parent).replaceChild(root, or);

                if (root.jjtGetNumChildren() > 0)
                    or.jjtAddChild(root, or.jjtGetNumChildren());
            }
        } else {
            for (QueryParserNode child : root)
                pullOutComplexTokensFromArrays(child);
        }
    }

}
