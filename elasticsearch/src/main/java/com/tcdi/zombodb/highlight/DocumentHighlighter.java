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
package com.tcdi.zombodb.highlight;

import com.tcdi.zombodb.query_parser.*;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.client.Client;
// import org.elasticsearch.client.ElasticsearchClient;

import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class DocumentHighlighter {

    private final Client client;
    private final ASTQueryTree query;
    private final Map<String, List<AnalyzedField>> analyzedFields = new HashMap<String, List<AnalyzedField>>() {
        @Override
        public List<AnalyzedField> get(Object key) {
            List<AnalyzedField> value = super.get(key);
            if (!super.containsKey(key))
                value = Collections.emptyList();
            return value;
        }
    };

    public DocumentHighlighter(Client client, String indexName, String primaryKeyFieldname, Map<String, Object> documentData, String queryString) throws ParseException {
        StringBuilder newQuery = new StringBuilder(queryString.length());
        Utils.extractArrayData(queryString, newQuery);
        QueryParser parser = new QueryParser(new StringReader(newQuery.toString().toLowerCase()));

        this.client = client;
        this.query = parser.parse(true);

        analyzeFields(parser, indexName, primaryKeyFieldname, documentData);
    }

    public List<AnalyzedField.Token> highlight() {
        highlight(this.query);

        List<AnalyzedField.Token> tokens = new ArrayList<>();
        for (List<AnalyzedField> fields : analyzedFields.values())
            for (AnalyzedField field : fields)
                tokens.addAll(field.get().reduce());

        return tokens;
    }

    private void highlight(ASTQueryTree query) {
        QueryParserNode root = query.getQueryNode();
        highlight(root);
    }

    private Highlighter _highlighter = new Highlighter();

    private class Highlighter {
        public void perform(QueryParserNode node) {
            if (node == null)
                return;

            String fieldname = node.getFieldname();
            if ("_all".equals(fieldname)) {
                for (List<AnalyzedField> fields : analyzedFields.values())
                    for (AnalyzedField field : fields)
                        field.get().keep(node);
            } else {
                for (AnalyzedField field : analyzedFields.get(fieldname))
                    field.get().keep(node);
            }
        }
    }

    private void highlight(QueryParserNode node) {
        if (node instanceof ASTWith)
            highlightChildren(node);
        else if (node instanceof ASTAnd)
            highlightChildren(node);
        else if (node instanceof ASTOr)
            highlightChildren(node);
        else if (node instanceof ASTExpansion)
            highlight(((ASTExpansion) node).getQuery());
        else if (node instanceof ASTBoolQuery) {
            ASTBoolQuery boolQuery = (ASTBoolQuery) node;

            if (boolQuery.getMust() != null)
                highlightChildren(boolQuery.getMust());
            if (boolQuery.getShould() != null)
                highlightChildren(boolQuery.getShould());
        } else if (node instanceof ASTNot)
            ;   // do nothing for ASTNot nodes
        else
            _highlighter.perform(node);
    }

    private void highlightChildren(Iterable<QueryParserNode> iterable) {
        for (QueryParserNode node : iterable)
            highlight(node);
    }

    /**
     * Use built-in Elasticsearch field analyzers to parse each field that was searched
     */
    private void analyzeFields(QueryParser parser, String indexName, String primaryKeyFieldname, Map<String, Object> documentData) {
        Set<String> usedFieldnames = parser.getUsedFieldnames();

        if (usedFieldnames.contains("_all")) {
            // get *all* the fields analyzed
            usedFieldnames.remove("_all");
            usedFieldnames.addAll(resolveAllFieldnames(null, documentData, new HashSet<String>()));
        }

        analyzeData(indexName, primaryKeyFieldname, usedFieldnames, null, documentData, documentData);
    }

    private void analyzeData(String indexName, String primaryKeyFieldname, Set<String> usedFieldnames, String baseFn, Map<String, Object> data, Map<String, Object> baseDocumentData) {
        for (String fieldName : usedFieldnames) {
            Object value;

            value = findValue(fieldName, data);
            if (value != null) {
                List<AnalyzedField> fields = analyzedFields.get(baseFn == null ? fieldName : baseFn + "." + fieldName);
                if (fields.isEmpty())
                    analyzedFields.put(baseFn == null ? fieldName : baseFn + "." + fieldName, fields = new ArrayList<>());

                int idx = 0;
                for (AnalyzedField af : fields) {
                    if (af.get().getFieldName().equals(baseFn + "." + fieldName))
                        idx++;
                }

                if (value instanceof List) {
                    for (Object element : (List) value) {

                        if (element instanceof Map) {
                            if (!fieldName.contains("."))
                                continue;
                            analyzeData(indexName, primaryKeyFieldname, resolveAllFieldnames(null, (Map) element, new HashSet<String>()), fieldName.substring(0, fieldName.lastIndexOf('.')), (Map) element, baseDocumentData);
                        } else {
                            // AnalyzeRequestBuilder rb = new AnalyzeRequestBuilder(client.admin().indices(), indexName, String.valueOf(element).toLowerCase());
                            AnalyzeRequestBuilder rb = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE, indexName, String.valueOf(element).toLowerCase());
                            //
                            rb.setAnalyzer("phrase");

                            try {
                                fields.add(new AnalyzedField(client, indexName, baseDocumentData.get(primaryKeyFieldname), baseFn == null ? fieldName : baseFn + "." + fieldName, idx++, client.admin().indices().analyze(rb.request()).get()));
                            } catch (InterruptedException | ExecutionException e) {
                                // ignore
                            }
                        }
                    }
                } else {
                    // AnalyzeRequestBuilder rb = new AnalyzeRequestBuilder(client.admin().indices(), indexName, String.valueOf(value).toLowerCase());
                    AnalyzeRequestBuilder rb = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE, indexName, String.valueOf(value).toLowerCase());
                    rb.setAnalyzer("phrase");

                    try {
                        fields.add(new AnalyzedField(client, indexName, baseDocumentData.get(primaryKeyFieldname), baseFn == null ? fieldName : baseFn + "." + fieldName, idx, client.admin().indices().analyze(rb.request()).get()));
                    } catch (InterruptedException | ExecutionException e) {
                        // ignore
                    }
                }
            }
        }
    }

    private static Set<String> resolveAllFieldnames(String base, Map<String, Object> data, Set<String> fieldnames) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fn = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                if (base == null)
                    resolveAllFieldnames(fn, (Map<String, Object>) value, fieldnames);
                else
                    resolveAllFieldnames(base + "." + fn, (Map<String, Object>) value, fieldnames);
            } else {
                if (base == null)
                    fieldnames.add(fn);
                else
                    fieldnames.add(base + "." + fn);
            }
        }

        return fieldnames;
    }

    private Object findValue(String fieldname, Map<String, Object> data) {
        int idx = fieldname.indexOf('.');
        if (idx < 0)
            return data.get(fieldname);
        else {
            Object value = data.get(fieldname.substring(0, idx));
            if (value instanceof Map) {
                return findValue(fieldname.substring(idx + 1), (Map<String, Object>) value);
            } else {
                return value;
            }
        }
    }
}
