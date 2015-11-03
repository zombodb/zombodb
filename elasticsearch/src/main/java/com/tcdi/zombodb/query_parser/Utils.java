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

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;

import java.io.StringReader;
import java.util.*;

/**
 * @author e_ridge
 */
public class Utils {

    public static class SubparseInfo {
        public final Iterable<QueryParserNode> nodes;
        public final int totalCount;

        public SubparseInfo(Iterable<QueryParserNode> nodes, int totalCount, QueryParserNode.Operator operator) {
            this.nodes = nodes;
            this.totalCount = totalCount;
            for (QueryParserNode node : nodes)
                forceOperator(node, operator);
        }

        private void forceOperator(QueryParserNode root, QueryParserNode.Operator operator) {
            root.setOperator(operator);
            for (QueryParserNode child : root)
                forceOperator(child, operator);
        }
    }

    public static String unescape(String s) {
        if (s == null || s.length() == 0)
            return s;

        StringBuilder sb  = new StringBuilder();
        char prev_ch = 0;
        for (int i=0, len=s.length(); i<len; i++) {
            char ch = s.charAt(i);

            switch(ch) {
                case '\\':
                    if (prev_ch == '\\')
                        sb.append(ch);  // only keep backslashes that are escaped
                    break;
                default:
                    sb.append(ch);
                    break;
            }

            prev_ch = ch;
        }

        return sb.toString();
    }

    public static boolean hasOnlyEscapedWildcards(ASTPhrase phrase) {
        char prev_ch = 0;
        String value = phrase.getEscapedValue();
        int esc = 0, unesc = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '*':
                case '?':
                case '~':
                    if (prev_ch == '\\') {
                        // contains an escaped wildcard
                        esc++;
                        break;
                    }
                    unesc++;
                    break;

            }

            prev_ch = ch;
        }

        return esc > 0 && unesc == 0;
    }

    public static int countValidWildcards(ASTPhrase phrase) {
        char prev_ch = 0;
        String value = phrase.getEscapedValue();
        int unesc = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '*':
                case '?':
                case '~':
                    if (prev_ch == '\\') {
                        // contains an escaped wildcard
                        break;
                    }
                    unesc++;
                    break;

            }

            prev_ch = ch;
        }

        return unesc;
    }

    public static String join(Collection<String> c) {
        StringBuilder sb = new StringBuilder();
        for (String s : c) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(s);
        }
        return sb.toString();
    }

    public static boolean isComplexTerm(String value) {
        char prevch = 0;
        for (int i = 0, len = value.length(); i < len; i++) {
            char ch = value.charAt(i);

            if (!Character.isLetterOrDigit(ch) && ch != '_' && ch != '\\' && prevch != '\\' && ch != '*' && ch != '?' && ch != '~')
                return true;
            prevch = ch;
        }
        return false;
    }

    public static List<String> simpleTokenize(String value) {
        List<String> l = new ArrayList<>();

        StringBuilder sb = new StringBuilder();
        char prevch = 0;
        for (int i=0, len=value.length(); i<len; i++) {
            char ch = value.charAt(i);

            if (!Character.isLetterOrDigit(ch) && ch != '_' && prevch != '\\' && ch != '*' && ch != '?' && ch != '~' && ch != '\'' && ch != '.' && ch != ':') {
                if (sb.length() > 0)
                    l.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(ch);
            }

            prevch = ch;
        }
        if (sb.length() > 0)
            l.add(sb.toString());

        return l;
    }

    public static SubparseInfo subparsePhrase(String escapedPhrase, String fieldname, QueryParserNode.Operator operator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < escapedPhrase.length(); i++) {
            char ch = escapedPhrase.charAt(i);

            if (Arrays.binarySearch(IndexMetadata.NEEDS_ESCAPES, ch) > -1) {
                char prev = i == 0 ? 0 : escapedPhrase.charAt(i-1);
                if (prev != '\\')
                    sb.append("\\").append(ch);
                else    // already escaped
                    sb.append(ch);
            } else {
                sb.append(ch);
            }
        }

        try {
            QueryParser qp = new QueryParser(new StringReader(sb.toString()));
            ASTQueryTree tree = qp.parse(false);
            tree.forceFieldname(fieldname);

            if (tree.jjtGetNumChildren() == 0)
                throw new QueryRewriter.QueryRewriteException("PHRASE is empty");
            else if (!(tree.getChild(0) instanceof ASTAnd))
                return new SubparseInfo(tree, tree.countNodes(), operator);

            return new SubparseInfo(tree.getChild(0), tree.getChild(0).countNodes(), operator);
        } catch (ParseException pe) {
            throw new QueryRewriter.QueryRewriteException(pe);
        }
    }

    public static QueryParserNode convertToProximity(ASTPhrase phrase) {
        String value = String.valueOf(phrase.getValue());
        List<String> tokens = Utils.simpleTokenize(value);
        // rewrite the phrase as a proximity query
        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            if (sb.length() > 0) {
                sb.append(" ");
                sb.append("w");
                if (phrase.isOrdered())
                    sb.append("o");
                sb.append("/");
                sb.append(phrase.getDistance());
                sb.append(" ");
            }
            sb.append(token.replaceAll("([" + IndexMetadata.NEEDS_ESCAPES_AS_STRING + "])", "\\\\$1"));
        }

        sb.insert(0, phrase.getFieldname() + ":(");
        sb.append(")");

        try {
            QueryParser qp = new QueryParser(new StringReader(sb.toString()));
            ASTQueryTree tree = qp.parse(true);
            if (tree.countNodes() == 1)
                return tree.getChild(0);
            QueryParserNode prox = tree.getChild(ASTProximity.class);
            if (prox == null)
                throw new RuntimeException("Phrase (" + sb.toString() + ") did not parse into a proximity chain");
            return (ASTProximity) prox;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ASTProximity convertToProximity(String fieldname, List<AnalyzeResponse.AnalyzeToken> tokens) {
        // rewrite the phrase as a proximity query
        StringBuilder sb = new StringBuilder();
        for (AnalyzeResponse.AnalyzeToken token : tokens) {
            if (sb.length() > 0) {
                sb.append(" ");
                sb.append("wo");
                sb.append("/0");
                sb.append(" ");
            }
            sb.append(token.getTerm().replaceAll("([" + IndexMetadata.NEEDS_ESCAPES_AS_STRING + "])", "\\\\$1"));
        }

        sb.insert(0, fieldname + ":(");
        sb.append(")");

        try {
            QueryParser qp = new QueryParser(new StringReader(sb.toString()));
            ASTQueryTree tree = qp.parse(false);
            QueryParserNode prox = tree.getChild(0);
            if (!(prox instanceof ASTProximity))
                throw new RuntimeException("Phrase (" + sb.toString() + ") did not parse into a proximity chain");
            return (ASTProximity) prox;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, StringBuilder> extractArrayData(String input, StringBuilder output) {
        Map<String, StringBuilder> arrayData = new HashMap<>();
        StringBuilder currentArray = null;
        String currentArrayName = null;
        boolean inArrayData = false;
        char nextChar;

        for (int i = 0, many = input.length(); i < many; i++) {
            char ch = Character.toLowerCase(input.charAt(i));
            nextChar = i < many - 1 ? input.charAt(i + 1) : 0;

            switch (ch) {
                case '[':
                    if (nextChar == '[' && !inArrayData) {
                        inArrayData = true;
                        currentArrayName = "$" + arrayData.size();
                        currentArray = new StringBuilder();
                        i++;
                    }
                    break;
                case ']':
                    if (nextChar == ']' && inArrayData) {
                        arrayData.put(currentArrayName, currentArray);
                        output.append("[[").append(currentArrayName).append("]");
                        inArrayData = false;
                        i++;
                    }
                    break;
            }

            if (inArrayData)
                currentArray.append(ch);
            else
                output.append(ch);
        }

        return arrayData;
    }

    public static String validateSameNestedPath(ASTWith node) {
        return validateSameNestedPath(node, null);
    }
    public static String validateSameNestedPath(QueryParserNode node, String nestedPath) {
        if (!node.hasChildren())
            return nestedPath;

        for (QueryParserNode child : node) {
            if (nestedPath == null)
                nestedPath = child.getNestedPath();

            if (child.hasChildren())
                nestedPath = validateSameNestedPath(child, nestedPath);
            else if (nestedPath != null && !nestedPath.equals(child.getNestedPath()))
                throw new RuntimeException ("WITH chain must all belong to the same nested object");
        }

        if (nestedPath == null)
            throw new RuntimeException ("WITH chain must all belong to a nested object");

        return nestedPath;
    }

}
