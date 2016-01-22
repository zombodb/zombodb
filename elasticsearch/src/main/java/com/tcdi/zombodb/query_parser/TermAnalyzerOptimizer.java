package com.tcdi.zombodb.query_parser;

import org.elasticsearch.client.Client;

/**
 * Created by e_ridge on 1/14/16.
 */
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
    }

    private void analyzeNodes(QueryParserNode root) {

        if (root instanceof ASTOptions || root instanceof ASTFieldLists || root instanceof ASTAggregate)
            return;

        if (root.isStringValue() && (root instanceof ASTWord || root instanceof ASTPhrase || root instanceof ASTFuzzy || root instanceof ASTPrefix || root instanceof ASTWildcard))
            analyzeToken(root);

        for(QueryParserNode child : root)
            analyzeNodes(child);

    }

    private void analyzeToken(QueryParserNode node) {
        switch (node.getOperator()) {
            case CONCEPT:
            case FUZZY_CONCEPT:
            case REGEX:
                return;
        }

        QueryParserNode newNode = Utils.rewriteToken(client, metadataManager, node);
        ((QueryParserNode) node.parent).replaceChild(node, newNode);
    }
}
