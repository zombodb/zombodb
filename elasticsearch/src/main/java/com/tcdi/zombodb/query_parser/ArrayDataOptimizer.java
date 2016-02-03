package com.tcdi.zombodb.query_parser;

import java.io.StringReader;
import java.util.Map;

/**
 * Created by e_ridge on 2/2/16.
 */
public class ArrayDataOptimizer {

    private final ASTQueryTree tree;
    private final IndexMetadataManager metadataManager;
    private final Map<String, StringBuilder> arrayData;

    public ArrayDataOptimizer(ASTQueryTree tree, IndexMetadataManager metadataManager, Map<String, StringBuilder> arrayData) {
        this.tree = tree;
        this.metadataManager = metadataManager;
        this.arrayData = arrayData;
    }

    public void optimize() {
        analyzeArrayData(tree);
    }

    private void analyzeArrayData(QueryParserNode root) {
        for (QueryParserNode child : root) {
            if (child instanceof ASTArrayData) {
                String fieldname = child.getFieldname();
                String analyzer = metadataManager.getMetadataForField(fieldname).getAnalyzer(fieldname);

                if (analyzer != null) {
                    // this field, which uses the double-bracket array syntax (ASTArrayData)
                    // is actually analyzed, so we need to convert it to a regular array, parse it,
                    // and replace the ASTArrayData node in the tree
                    StringBuilder arrayString = arrayData.get(child.getValue().toString());
                    StringBuilder arrayQuery = new StringBuilder(arrayString.length());

                    arrayQuery.append(fieldname).append(":[").append(arrayString).append("]");

                    try {
                        QueryParser qp = new QueryParser(new StringReader(arrayQuery.toString()));
                        ASTQueryTree tree = qp.parse(false);
                        root.replaceChild(child, tree.getQueryNode());
                    } catch (Exception e) {
                        throw new RuntimeException("Problem subparsing ArrayData", e);
                    }
                }
            } else {
                analyzeArrayData(child);
            }
        }
    }

}
