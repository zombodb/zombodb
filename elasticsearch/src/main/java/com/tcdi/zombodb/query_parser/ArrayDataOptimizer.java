package com.tcdi.zombodb.query_parser;

import java.io.StringReader;
import java.util.Locale;
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
                String analyzer = metadataManager.getMetadataForField(fieldname).getSearchAnalyzer(fieldname);

                if (analyzer != null) {
                    if ("exact".equals(analyzer)) {
                        // we know the definition of the "exact" analyzer, and it forces things to lower-case
                        // so we'll short-circuit analyzing each term and just force the arraydata string to lower-case
                        String key = child.getValue().toString();
                        StringBuilder arrayString = arrayData.get(key);
                        arrayData.put(key, new StringBuilder(arrayString.toString().toLowerCase(Locale.ENGLISH)));
                    } else {
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
                }
            } else {
                analyzeArrayData(child);
            }
        }
    }

}
