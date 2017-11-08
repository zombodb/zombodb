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
package llc.zombodb.highlight;

import llc.zombodb.query_parser.*;
import llc.zombodb.query_parser.metadata.IndexMetadataManager;
import llc.zombodb.query_parser.utils.Utils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;

import java.util.*;

public class AnalyzedField {

    private static class ProximityGroup {
        Stack<Token> tokens = new Stack<>();
        int min_pos;
        int max_pos;

        public ProximityGroup(Token leftToken, Token rightToken, int min_pos, int max_pos) {
            tokens.push(rightToken);
            tokens.push(leftToken);
            this.min_pos = min_pos;
            this.max_pos = max_pos;
        }
    }

    public static class Token extends AnalyzeResponse.AnalyzeToken {
        private final Object primaryKey;
        private final String fieldName;
        private final int arrayIndex;
        private boolean keep = false;
        private String clause;
        private ProximityGroup group = null;

        public Token(Object primaryKey, String fieldName, int arrayIndex, AnalyzeResponse.AnalyzeToken token) {
            super(token.getTerm(), token.getPosition()+1, token.getStartOffset(), token.getEndOffset(), 0, token.getType(), null);
            this.primaryKey = primaryKey;
            this.fieldName = fieldName;
            this.arrayIndex = arrayIndex;
        }

        public boolean shouldKeep() {
            return keep;
        }

        public void setKeep(QueryParserNode node) {
            this.keep = true;
            clause = node.getDescription();
        }

        //
        // NB:  These getters are used when we're serialized to JSON
        //

        public String getFieldName() {
            return fieldName;
        }

        @SuppressWarnings("unused")
        public Object getPrimaryKey() {
            return primaryKey;
        }

        @SuppressWarnings("unused")
        public int getArrayIndex() {
            return arrayIndex;
        }

        @SuppressWarnings("unused")
        public String getClause() {
            return clause;
        }

        @Override
        public String toString() {
            return fieldName + ":#" + getPosition()+1 + " (" + getStartOffset() + "," + getEndOffset() + ")" + getType() + ": " + getTerm();
        }
    }

    public class Proxy {

        public String getFieldName() {
            return fieldName;
        }

        public void keep(QueryParserNode node) {
            if (node instanceof ASTWord)
                keep((ASTWord) node);
            else if (node instanceof ASTNumber)
                keep((ASTNumber) node);
            else if (node instanceof ASTBoolean)
                keep((ASTBoolean) node);
            else if (node instanceof ASTArray)
                keep((ASTArray) node);
            else if (node instanceof ASTPhrase)
                keep((ASTPhrase) node);
            else if (node instanceof ASTPrefix)
                keep((ASTPrefix) node);
            else if (node instanceof ASTWildcard)
                keep((ASTWildcard) node);
            else if (node instanceof ASTProximity)
                keep((ASTProximity) node);
            else if (node instanceof ASTNotNull)
                keep((ASTNotNull) node);
            else if (node instanceof ASTNull)
                keep((ASTNull) node);
            else if (node instanceof ASTFuzzy)
                keep((ASTFuzzy) node);
            else
                /*just ignore it*/;
        }

        public void keep(ASTWord word) {
            String value = String.valueOf(word.getValue());

            if (word.getOperator() == QueryParserNode.Operator.REGEX) {
                keepRegex(word);
            } else {
                try {
                    if (Utils.isComplexTerm(value)) {
                        if (client == null)
                            return;
                        AnalyzeResponse response = analyzePhrase(value);
                        if (response.getTokens().size() > 1) {
                            ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);
                            phrase.setFieldname(fieldName);
                            phrase.setValue(value);
                            phrase.setBeenSubparsed(true);
                            phrase.setOperator(word.getOperator());
                            keep(phrase);
                        } else {
                            for (Token token : match(word))
                                token.setKeep(word);
                        }
                    } else {
                        for (Token token : match(word))
                            token.setKeep(word);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void keep(ASTNumber number) {
            for (Token token : match(number))
                token.setKeep(number);
        }

        public void keep(ASTBoolean bool) {
            for (Token token : match(bool))
                token.setKeep(bool);
        }

        public void keep(ASTArray array) {
            for (QueryParserNode child : array)
                keep(child);
        }

        public void keep(ASTPhrase phrase) {
            String value = String.valueOf(phrase.getValue());
            final List<String> tokens = Utils.simpleTokenize(value);
            QueryParserNode toKeep;

            if (phrase.getOperator() == QueryParserNode.Operator.REGEX) {
                keepRegex(phrase);
                return;
            }

            if (!phrase.isBeenSubparsed() && tokens.size() == 1 && !Utils.isComplexTerm(value)) {
                // single-token phrase, so rewrite as a word
                try {
                    AnalyzeResponse analyzed = analyzePhrase(tokens.get(0));

                    toKeep = new ASTWord(QueryParserTreeConstants.JJTWORD) {
                        @Override
                        public String getDescription() {
                            return fieldname + " " + operator + " " + "\"" + tokens.get(0) + "\"";
                        }
                    };
                    if (analyzed.iterator().hasNext())
                        toKeep.setValue(analyzed.iterator().next().getTerm());
                    else
                        toKeep.setValue(tokens.get(0));

                    toKeep.setFieldname(fieldName);
                    toKeep.setOperator(phrase.getOperator());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (value.contains("*") || value.contains("?") || value.contains("*")) {
                    toKeep = Utils.convertToProximityForHighlighting(metadataManager, phrase);
                } else {
                    try {
                        if (client == null)
                            return;
                        AnalyzeResponse response = analyzePhrase(value);
                        toKeep = Utils.convertToProximityForHighlighting(metadataManager, phrase.getFieldname(), response.getTokens());
                    } catch (Exception e2) {
                        try {
                            toKeep = Utils.convertToProximityForHighlighting(metadataManager, phrase);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

            keep(toKeep);
        }

        public void keep(ASTPrefix prefix) {
            for (Token token : match(prefix)) {
                token.setKeep(prefix);
            }
        }

        public void keep(ASTWildcard wildcard) {
            for (Token token : match(wildcard)) {
                token.setKeep(wildcard);
            }
        }

        public void keep(ASTNotNull notnull) {
            // noop
        }

        public void keep(ASTNull _null) {
            // noop
        }

        public void keep(ASTFuzzy fuzzy) {
            // TODO:  implement this
        }

        public void keep(ASTProximity proximity) {
            List<ProximityGroup> scratch = new ArrayList<>();
            keep(proximity, scratch);
            int cnt = proximity.getChildrenOfType(ASTWord.class).size() / 2 + 1;
            for (ProximityGroup pair : scratch) {
                for (Token token : pair.tokens) {
                    if (token.shouldKeep() || pair.tokens.size() >= cnt)
                        token.setKeep(proximity);
                    if (token.group != null) {
                        for (Token other : token.group.tokens)
                            other.setKeep(proximity);
                    }
                }
            }
        }

        public void keep(ASTProximity proximity, List<ProximityGroup> scratch) {
            proximity.forceFieldname(proximity.getFieldname());
            QueryParserNode left = proximity.getChild(0);
            QueryParserNode right = proximity.getChild(1);

            left.setOperator(proximity.getOperator());
            right.setOperator(proximity.getOperator());
            if (right instanceof ASTProximity || left instanceof ASTProximity) {
                int distance = proximity.getDistance();
                boolean ordered = proximity.isOrdered();

                if (left instanceof ASTProximity) {
                    // swap left and right
                    QueryParserNode tmp = right;
                    right = left;
                    left = tmp;
                }
                List<Token> leftTokens = match(left);

                keep((ASTProximity) right, scratch);

                for (int i = leftTokens.size() - 1; i >= 0; --i) {    // walk backwards so we always find the closest term first
                    boolean foundClosestMatch = false;
                    Token lt = leftTokens.get(i);
                    for (ProximityGroup pair : scratch) {
                        if (ordered) {
                            int min_pos = pair.min_pos - (distance + 1);
                            int max_pos = pair.min_pos;
                            int pos = lt.getPosition()+1;

                            if (pos >= min_pos && pos < max_pos) {
                                pair.tokens.push(lt);
                                if (!foundClosestMatch) {
                                    pair.min_pos = Math.min(pair.min_pos, lt.getPosition()+1);
                                    pair.max_pos = Math.max(pair.max_pos, lt.getPosition()+1);
                                    foundClosestMatch = true;
                                }
                            }
                        } else {
                            int min_pos = pair.min_pos - (distance + 1);
                            int max_pos = pair.max_pos + (distance);
                            int pos = lt.getPosition()+1;

                            if (pos >= min_pos && pos <= max_pos) {
                                pair.tokens.push(lt);
                                pair.min_pos = Math.min(pair.min_pos, lt.getPosition()+1);
                                pair.max_pos = Math.max(pair.max_pos, lt.getPosition()+1);
                            }
                        }
                    }
                }
            } else {
                int distance = proximity.getDistance();
                boolean ordered = proximity.isOrdered();
                List<Token> leftTokens = match(left);
                List<Token> rightTokens = match(right);

                for (Token lt : leftTokens) {
                    for (Token rt : rightTokens) {
                        int min_pos = lt.getPosition()+1;
                        int max_pos = rt.getPosition()+1;
                        int diff = max_pos - min_pos;

                        if (ordered && diff < 0)
                            continue;

                        if (Math.abs(diff) - 1 <= distance) {
                            if (lt.group != null)
                                scratch.add(lt.group);
                            if (rt.group != null)
                                scratch.add(rt.group);
                            scratch.add(new ProximityGroup(lt, rt, Math.min(lt.getPosition()+1, rt.getPosition()+1), Math.max(lt.getPosition()+1, rt.getPosition()+1)));
                        }
                    }
                }
            }
        }

        public void keepRegex(QueryParserNode node) {
            for (Token token : matchRegex(node)) {
                token.setKeep(node);
            }
        }

        public List<Token> reduce() {
            if (terms.size() == 0)
                return Collections.emptyList();

            List<Token> tokens = new ArrayList<>();

            for (List<Token> values : terms.values()) {
                for (Token t : values) {
                    if (t.shouldKeep())
                        tokens.add(t);
                }
            }

            return tokens;
        }
    }

    private final Client client;
    private final IndexMetadataManager metadataManager;
    private final String indexName;
    private final Object primaryKey;
    private final String fieldName;
    private final int arrayIndex;
    private final AnalyzeResponse analysis;
    private final Map<String, List<Token>> terms = new HashMap<String, List<Token>>() {
        @Override
        public List<Token> get(Object key) {
            List<Token> rc = super.get(key);
            if (rc == null)
                rc = Collections.emptyList();
            return rc;
        }
    };
    private Proxy proxy = null;

    public AnalyzedField(Client client, IndexMetadataManager metadataManager, String indexName, Object primaryKey, String fieldName, int arrayIndex, AnalyzeResponse analysis) {
        this.client = client;
        this.metadataManager = metadataManager;
        this.indexName = indexName;
        this.primaryKey = primaryKey;
        this.fieldName = fieldName;
        this.arrayIndex = arrayIndex;
        this.analysis = analysis;
    }

    public Proxy get() {
        if (proxy == null) {
            for (AnalyzeResponse.AnalyzeToken token : analysis) {
                String word = token.getTerm();
                List<Token> list = terms.get(word);
                if (list == Collections.EMPTY_LIST)
                    terms.put(word, list = new ArrayList<>());

                list.add(new Token(primaryKey, fieldName, arrayIndex, token));
            }

            proxy = new Proxy();
        }

        return proxy;
    }


    private List<Token> match(QueryParserNode node) {
        if (node instanceof ASTWord)
            return match((ASTWord) node);
        else if (node instanceof ASTPrefix)
            return match((ASTPrefix) node);
        else if (node instanceof ASTWildcard)
            return match((ASTWildcard) node);
        else if (node instanceof ASTNumber)
            return match((ASTNumber) node);
        else if (node instanceof ASTBoolean)
            return match((ASTBoolean) node);
        else if (node instanceof ASTArray)
            return match((ASTArray) node);
        else if (node instanceof ASTPhrase)
            return match((ASTPhrase) node);
        else if (node instanceof ASTOr)
            return match((ASTOr) node);
        else if (node instanceof ASTNotNull) {
            ASTWildcard wildcard = new ASTWildcard(QueryParserTreeConstants.JJTWILDCARD);
            wildcard.setValue("*");
            return match(wildcard);
        } else
            throw new RuntimeException("Don't know how to match node type: " + node.getClass().getSimpleName());
    }

    private List<Token> match(ASTWord word) {
        if (word.getOperator() == QueryParserNode.Operator.REGEX)
            return matchRegex(word);

        String value = String.valueOf(word.getValue());
        while (value.length() > 0 && value.endsWith("."))
            value = value.substring(0, value.length() - 1);
        while (value.length() > 0 && value.startsWith("."))
            value = value.substring(1);

        List<Token> matches = terms.get(value);
        if (matches.size() == 0) {
            if (Utils.isComplexTerm(value)) {
                try {
                    value = value.replaceAll("[^A-Za-z0-9_]", " ");
                    ASTPhrase phrase = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);
                    phrase.setFieldname(word.getFieldname());
                    phrase.setValue(value);

                    return match(phrase);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return matches;
    }

    private List<Token> match(ASTNumber number) {
        return terms.get(String.valueOf(number.getValue()));
    }

    private List<Token> match(ASTBoolean bool) {
        return terms.get(String.valueOf(bool.getValue()));
    }

    private List<Token> match(ASTArray array) {
        List<Token> l = new ArrayList<>();
        for (QueryParserNode child : array) {
            l.addAll(match(child));
        }
        return l;
    }

    private List<Token> match(ASTPrefix prefix) {
        List<Token> l = new ArrayList<>();
        String value = String.valueOf(prefix.getValue());

        for (Map.Entry<String, List<Token>> entry : terms.entrySet()) {
            String term = entry.getKey();
            if (term.startsWith(value))
                l.addAll(entry.getValue());
        }
        return l;
    }

    private List<Token> match(ASTWildcard wildcard) {
        StringBuilder sb = new StringBuilder();
        char prevch = 0;
        for (int i = 0; i < wildcard.getEscapedValue().length(); i++) {
            char ch = wildcard.getEscapedValue().charAt(i);

            switch (ch) {
                case '*':
                case '?':
                    if (prevch == '\\') {
                        sb.append(ch);
                    } else {
                        sb.append(".").append(ch);
                    }
                    break;
                default:
                    sb.append(ch);
            }

            prevch = ch;
        }

        List<Token> l = new ArrayList<>();
        for (Map.Entry<String, List<Token>> entry : terms.entrySet()) {
            String term = entry.getKey();
            String pattern = sb.toString().replaceAll("[\\\\]", "");
            if (term.matches(pattern))
                l.addAll(entry.getValue());
        }
        return l;
    }

    private List<Token> match(ASTPhrase phrase) {
        if (phrase.getOperator() == QueryParserNode.Operator.REGEX)
            return matchRegex(phrase);

        QueryParserNode node = Utils.convertToProximityForHighlighting(metadataManager, phrase);
        if (node instanceof ASTProximity) {
            ASTProximity prox = (ASTProximity) node;
            List<ProximityGroup> scratch = new ArrayList<>();

            get().keep(prox, scratch);

            List<Token> l = new ArrayList<>();
            for (ProximityGroup group : scratch) {
                Token first = group.tokens.get(0);
                Token last = group.tokens.peek();

                first.group = group;
                last.group = group;

                l.add(last);
                l.add(first);
            }

            return l;
        } else {
            return match(node);
        }
    }

    private List<Token> match(ASTOr or) {
        List<Token> tokens = new ArrayList<>();
        for(QueryParserNode child : or) {
            tokens.addAll(match(child));
        }

        return tokens;
    }

    private List<Token> matchRegex(QueryParserNode node) {
        String regex = node.getEscapedValue();
        List<Token> l = new ArrayList<>();
        for (Map.Entry<String, List<Token>> entry : terms.entrySet()) {
            String term = entry.getKey();
            if (term.matches(regex))
                l.addAll(entry.getValue());
        }
        return l;
    }


    private AnalyzeResponse analyzePhrase(String value) {
        try {
            // AnalyzeRequest request = new AnalyzeRequestBuilder(client.admin().indices(), indexName, String.valueOf(value).toLowerCase()).setAnalyzer("phrase").request();
            AnalyzeRequest request = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE, indexName, String.valueOf(value).toLowerCase()).setAnalyzer("phrase").request();
            return client.admin().indices().analyze(request).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
