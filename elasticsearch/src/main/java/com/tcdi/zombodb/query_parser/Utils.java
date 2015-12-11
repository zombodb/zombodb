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

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;

import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author e_ridge
 */
public class Utils {

    public static String unescape(String s) {
        if (s == null || s.length() == 0)
            return s;

        StringBuilder sb  = new StringBuilder();
        for (int i=0, len=s.length(); i<len; i++) {
            char ch = s.charAt(i);
            if (ch == '\\') {
                char next = i<len-1 ? s.charAt(++i) : 0;
                if (next == 0)
                    throw new RuntimeException("Invalid escape sequence at end of string");

                sb.append(next);
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    public static int countValidWildcards(String phrase) {
        int unesc = 0;
        boolean inesc = false;

        for (int i = 0; i < phrase.length(); i++) {
            char ch = phrase.charAt(i);

            switch (ch) {
                case '*':
                case '?':
                case '~':
                    if (inesc) {
                        // contains an escaped wildcard
                        break;
                    }
                    unesc++;
                    break;
            }

            inesc = !inesc && ch == '\\';
        }

        return unesc;
    }

    public static QueryParserNode convertToWildcardNode(String fieldname, QueryParserNode.Operator operator, String value) {
        QueryParserNode node;
        int wildcardCount = countValidWildcards(value);

        if (wildcardCount == 0) {
            node = new ASTWord(QueryParserTreeConstants.JJTWORD);
            node.setValue(value);
        } else if (wildcardCount == value.length()) {
            node = new ASTNotNull(QueryParserTreeConstants.JJTNOTNULL);
            node.setValue(value);
        } else if (wildcardCount > 1) {
            node = new ASTWildcard(QueryParserTreeConstants.JJTWILDCARD);
            node.setValue(value);
        } else if (value.endsWith("*") && (!value.endsWith("\\*") || value.endsWith("\\\\*"))) {
            node = new ASTPrefix(QueryParserTreeConstants.JJTPREFIX);
            node.setValue(value.substring(0, value.length()-1));
        } else if (value.endsWith("?") && (!value.endsWith("\\?") || value.endsWith("\\\\?"))) {
            node = new ASTWildcard(QueryParserTreeConstants.JJTWILDCARD);
            node.setValue(value);
        } else if (value.endsWith("~") && (!value.endsWith("\\~") || value.endsWith("\\\\~"))) {
            node = new ASTFuzzy(QueryParserTreeConstants.JJTFUZZY);
            node.setValue(value.substring(0, value.length() - 1));
        } else if (value.matches("^.*~\\d+$")) {
            Pattern p = Pattern.compile("^(.*)~(\\d+)$");
            Matcher m = p.matcher(value);

            if (m.find()) {
                node = new ASTFuzzy(QueryParserTreeConstants.JJTFUZZY);
                node.fuzzyness = Integer.valueOf(m.group(2));
                node.setValue(m.group(1));
            } else {
                throw new RuntimeException("Unable to determine fuzziness");
            }
        } else {
            node = new ASTWildcard(QueryParserTreeConstants.JJTWILDCARD);
            node.setValue(value);
        }

        node.setFieldname(fieldname);
        node.setOperator(operator);

        return node;
    }

    public static String join(Collection<String> c) {
        return join(c, " ");
    }

    public static String join(Collection<String> c, String sep) {
        StringBuilder sb = new StringBuilder();
        for (String s : c) {
            if (sb.length() > 0) sb.append(sep);
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

    public static List<String> tokenizePhrase(Client client, IndexMetadataManager metadataManager, String fieldname, String phrase) throws RuntimeException {
        String analyzer = metadataManager.getMetadataForField(fieldname).getAnalyzer(fieldname);
        if (analyzer == null)
            analyzer = "default";

        analyzer += "_search";
        try {
            AnalyzeResponse response = client.admin().indices().analyze(
                    new AnalyzeRequestBuilder(
                            client.admin().indices(),
                            metadataManager.getMetadataForField(fieldname).getLink().getIndexName(),
                            phrase
                    ).setAnalyzer(analyzer).request()
            ).get();

            List<String> tokens = new ArrayList<>();
            for (AnalyzeResponse.AnalyzeToken t : response) {
                tokens.add(t.getTerm());
            }

            return tokens;
        } catch (Exception e) {
            throw new RuntimeException(e);
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
