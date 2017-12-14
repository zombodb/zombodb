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
package llc.zombodb.query_parser.utils;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import llc.zombodb.query_parser.*;
import llc.zombodb.query_parser.metadata.IndexMetadataManager;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.StringReader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static final char[] NEEDS_ESCAPES = new char[]{'A', 'a', 'O', 'o', 'W', 'w', '\t', '\n', '\r', '\f', '$', '^', '/', ':', '=', '<', '>', '!', '#', '@', '(', ')', '"', '\'', '.', ',', '&', '[', ']'};
    private static final String NEEDS_ESCAPES_AS_STRING;

    static {
        Arrays.sort(NEEDS_ESCAPES);

        StringBuilder sb = new StringBuilder();
        for (char ch : NEEDS_ESCAPES) {
            switch (ch) {
                case '[':
                case ']':
                case '-':
                case '\\':
                    sb.append("\\");
                    break;
            }
            sb.append(ch);
        }
        NEEDS_ESCAPES_AS_STRING = sb.toString();
    }

    public static String unescape(String s) {
        if (s == null || s.length() <= 1)
            return s;

        StringBuilder sb = new StringBuilder();
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch == '\\') {
                char next = i < len - 1 ? s.charAt(++i) : 0;
                if (next == 0)
                    break; // don't leave any dangling escape characters -- they're insignificant

                sb.append(next);
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    private static int countValidWildcards(String phrase) {
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

    private static int countValidStarWildcards(String phrase) {
        int unesc = 0;
        boolean inesc = false;

        for (int i = 0; i < phrase.length(); i++) {
            char ch = phrase.charAt(i);

            switch (ch) {
                case '*':
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

    private static QueryParserNode convertToWildcardNode(String fieldname, QueryParserNode.Operator operator, String value) {
        QueryParserNode node;
        int wildcardCount = countValidWildcards(value);

        if (wildcardCount == 0) {
            node = new ASTWord(QueryParserTreeConstants.JJTWORD);
            node.setValue(value);
        } else if (countValidStarWildcards(value) == value.length() || (wildcardCount == 1 && value.length() == 1)) {
            node = new ASTNotNull(QueryParserTreeConstants.JJTNOTNULL);
            node.setValue(value);
        } else if (wildcardCount > 1) {
            node = new ASTWildcard(QueryParserTreeConstants.JJTWILDCARD);
            node.setValue(value);
        } else if (value.endsWith("*") && (!value.endsWith("\\*") || value.endsWith("\\\\*"))) {
            node = new ASTPrefix(QueryParserTreeConstants.JJTPREFIX);
            node.setValue(value.substring(0, value.length() - 1));
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
                node.setFuzzyness(Integer.valueOf(m.group(2)));
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

    private static String join(Collection<String> c) {
        return join(c, " ");
    }

    private static String join(Collection<String> c, String sep) {
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
        for (int i = 0, len = value.length(); i < len; i++) {
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

    public static List<String> analyzeForSearch(Client client, IndexMetadataManager metadataManager, String fieldname, String phrase) throws RuntimeException {
        String analyzer = metadataManager.getMetadataForField(fieldname).getSearchAnalyzer(fieldname);
        return analyze(client, metadataManager, analyzer, fieldname, phrase);
    }

    private static List<String> analyzeForIndex(Client client, IndexMetadataManager metadataManager, String fieldname, String phrase) throws RuntimeException {
        String analyzer = metadataManager.getMetadataForField(fieldname).getIndexAnalyzer(fieldname);
        return analyze(client, metadataManager, analyzer, fieldname, phrase);
    }

    private static List<String> analyze(Client client, IndexMetadataManager metadataManager, String analyzer, String fieldname, String phrase) throws RuntimeException {
        if (analyzer == null)
            return Collections.singletonList(phrase);

        try {
            AnalyzeResponse response = client.admin().indices().analyze(
                    AnalyzeAction.INSTANCE.newRequestBuilder(client)
                    .setIndex(metadataManager.getMetadataForField(fieldname).getLink().getIndexName())
                    .setText(phrase)
                    .setAnalyzer(analyzer).request()
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

    public static QueryParserNode convertToProximityForHighlighting(IndexMetadataManager metadataManager, ASTPhrase phrase) {
        return convertToProximityForHighlighting(metadataManager, phrase.getFieldname(), Utils.simpleTokenize(String.valueOf(phrase.getValue())));
    }

    public static QueryParserNode convertToProximityForHighlighting(IndexMetadataManager metadataManager, String fieldname, final List<AnalyzeResponse.AnalyzeToken> tokens) {
        return convertToProximityForHighlighting(metadataManager, fieldname, () -> {
            final Iterator<AnalyzeResponse.AnalyzeToken> iterator = tokens.iterator();
            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public String next() {
                    return iterator.next().getTerm();
                }

                @Override
                public void remove() {
                    iterator.remove();
                }
            };
        });
    }

    private static QueryParserNode convertToProximityForHighlighting(IndexMetadataManager metadataManager, String fieldname, Iterable<String> tokens) {
        // rewrite the phrase as a proximity visibility_query
        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            if (sb.length() > 0) {
                sb.append(" ");
                sb.append("wo");
                sb.append("/0");
                sb.append(" ");
            }
            sb.append(token.replaceAll("([" + NEEDS_ESCAPES_AS_STRING + "])", "\\\\$1"));
        }

        sb.insert(0, fieldname + ":(");
        sb.append(")");

        try {
            QueryParser qp = new QueryParser(new StringReader(sb.toString()));
            ASTQueryTree tree = qp.parse(metadataManager, false);
            return tree.getChild(0);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static ASTProximity convertToProximity(String fieldname, final List<AnalyzeResponse.AnalyzeToken> tokens) {
        return convertToProximity(fieldname, () -> {
            final Iterator<AnalyzeResponse.AnalyzeToken> iterator = tokens.iterator();
            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public String next() {
                    return iterator.next().getTerm();
                }

                @Override
                public void remove() {
                    iterator.remove();
                }
            };
        });
    }

    public static ASTProximity convertToProximity(String fieldname, Iterable<String> tokens) {
        return convertToProximity(fieldname, tokens, 0);
    }

    private static ASTProximity convertToProximity(String fieldname, Iterable<String> tokens, int distance) {
        ASTProximity prox = new ASTProximity(QueryParserTreeConstants.JJTPROXIMITY);

        prox.setFieldname(fieldname);
        prox.setDistance(distance);

        for (String token : tokens) {
            QueryParserNode node = convertToWildcardNode(fieldname, QueryParserNode.Operator.CONTAINS, token);
            prox.jjtAddChild(node, prox.jjtGetNumChildren());
        }

        return prox;
    }

    public static Map<String, String> extractArrayData(String input, StringBuilder output) {
        Map<String, String> arrayData = new HashMap<>();
        boolean inArrayData = false;
        int arrStart = -1;
        int blockStart = -1;

        for (int i = 0, many = input.length(); i < many; i++) {
            char ch = input.charAt(i);
            char nextChar = i < many - 1 ? input.charAt(i + 1) : 0;

            switch (ch) {
                case '[':
                    if (nextChar == '[' && !inArrayData) {
                        output.append(input.substring(blockStart, i));
                        blockStart = -1;
                        inArrayData = true;
                        i++;
                        arrStart = i + 1;  // plus one to skip the double brackets at start of array: [[
                    }
                    break;
                case ']':
                    if (nextChar == ']' && inArrayData) {
                        String arrayName = "$" + arrayData.size();
                        arrayData.put(arrayName, input.substring(arrStart, i));
                        if (blockStart != -1) {
                            output.append(input.substring(blockStart, i));
                            blockStart = -1;
                        }
                        output.append("[[").append(arrayName).append("]]");
                        inArrayData = false;
                        i++;
                    }
                    break;
                default:
                    if (!inArrayData && blockStart == -1)
                        blockStart = i;
                    break;
            }
        }

        if (blockStart != -1)
            output.append(input.substring(blockStart, input.length()));

        return arrayData;
    }

    public static String validateSameNestedPath(ASTWith node) {
        return validateSameNestedPath(node, null);
    }

    private static String validateSameNestedPath(QueryParserNode node, String nestedPath) {
        if (!node.hasChildren())
            return nestedPath;

        for (QueryParserNode child : node) {
            if (nestedPath == null)
                nestedPath = child.getNestedPath();

            if (child.hasChildren())
                nestedPath = validateSameNestedPath(child, nestedPath);
            else if (nestedPath != null && !nestedPath.equals(child.getNestedPath()))
                throw new RuntimeException("WITH chain must all belong to the same nested object");
        }

        if (nestedPath == null)
            throw new RuntimeException("WITH chain must all belong to a nested object");

        return nestedPath;
    }

    public static QueryParserNode rewriteToken(Client client, IndexMetadataManager metadataManager, QueryParserNode node) throws RuntimeException {
        List<String> initialAnalyze;
        boolean hasWildcards = node instanceof ASTFuzzy;
        String input = node.getEscapedValue();
        String newToken;
        boolean isComplex;

        if (node instanceof ASTPrefix)
            input += "*";

        isComplex = Utils.isComplexTerm(input);
        input = input.replaceAll("[*]", "zdb_star_zdb");
        input = input.replaceAll("[?]", "zdb_question_zdb");
        input = input.replaceAll("[~]", "zdb_tilde_zdb");
        input = input.replaceAll("[\\\\]", "zdb_escape_zdb");

        // if the input token doesn't have any wildcards (or escapes)...
        if (input.equals(node.getEscapedValue())) {
            // and it uses a build-in analyzer...
            String analyzer = metadataManager.getMetadataForField(node.getFieldname()).getSearchAnalyzer(node.getFieldname());
            if (("exact".equals(analyzer) || "phrase".equals(analyzer) || "fulltext".equals(analyzer) || "fulltext_with_shingles".equals(analyzer))
                    && !isComplex) { // ... and is a single term

                // then we'll just convert it to lowercase
                node.setValue(input.toLowerCase());

                // and return without bothering to actually run it through an analyzer
                return node;
            }
        }

        initialAnalyze = analyzeForSearch(client, metadataManager, node.getFieldname(), input);
        if (initialAnalyze.isEmpty()) {
            initialAnalyze = analyzeForIndex(client, metadataManager, node.getFieldname(), input);
        }

        newToken = join(initialAnalyze);
        newToken = newToken.replaceAll("zdb_star_zdb", "*");
        newToken = newToken.replaceAll("zdb_question_zdb", "?");
        newToken = newToken.replaceAll("zdb_tilde_zdb", "~");
        newToken = newToken.replaceAll("zdb_escape_zdb", "\\\\");

        hasWildcards |= Utils.countValidWildcards(newToken) > 0;

        if (hasWildcards && !isComplex) {
            String analyzer = metadataManager.getMetadataForField(node.getFieldname()).getSearchAnalyzer(node.getFieldname());
            if (analyzer != null && analyzer.contains("_with_shingles")) {
                QueryParserNode tmp = Utils.convertToWildcardNode(node.getFieldname(), node.getOperator(), newToken);
                if (tmp instanceof ASTNotNull)
                    return tmp;
                boolean isNE = node.getOperator() == QueryParserNode.Operator.NE;

                node.setOperator(QueryParserNode.Operator.REGEX);
                newToken = newToken.replaceAll("[*]", "\\[\\^\\$\\]\\*");
                newToken = newToken.replaceAll("[?]", "\\[\\^\\$\\]");
                node.setValue(newToken);

                if (isNE) {
                    ASTNot not = new ASTNot(QueryParserTreeConstants.JJTNOT);
                    not.jjtAddChild(node, 0);
                    return not;
                }

                return node;
            }
        }


        QueryParserNode rc;
        if (!hasWildcards) {
            if (initialAnalyze.size() <= 1) {
                if (node instanceof ASTPrefix) {
                    rc = node;
                } else {
                    rc = new ASTWord(QueryParserTreeConstants.JJTWORD);
                }
            } else {
                rc = new ASTPhrase(QueryParserTreeConstants.JJTPHRASE);

                // because phrases go through analysis we want to just use whatever the user
                // provided in this case
                newToken = node.getEscapedValue();
            }

            rc.setIndexLink(node.getIndexLink());
            rc.setFieldname(node.getFieldname());
            rc.setOperator(node.getOperator());
            rc.setValue(newToken);
            rc.setFuzzyness(node.getFuzzyness());
            rc.setOrdered(node.isOrdered());
            rc.setDistance(node.getDistance());
            rc.setBoost(node.getBoost());
        } else {
            if (node instanceof ASTFuzzy)
                newToken += "~" + (node.isOrdered() ? "" : "!") + node.getFuzzyness();

            if (initialAnalyze.size() <= 1) {
                rc = Utils.convertToWildcardNode(node.getFieldname(), node.getOperator(), newToken);
            } else {
                rc = Utils.convertToProximity(node.getFieldname(), Arrays.asList(newToken.split("[ ]+")));
                if (rc.jjtGetNumChildren() == 1)
                    rc = (QueryParserNode) rc.jjtGetChild(0);
            }

            rc.setIndexLink(node.getIndexLink());
            rc.setFieldname(node.getFieldname());
            rc.setOperator(node.getOperator());
            rc.setFuzzyness(node.getFuzzyness());
            rc.setOrdered(node.isOrdered());
            rc.setDistance(node.getDistance());
            rc.setBoost(node.getBoost());
        }

        if (rc instanceof ASTPrefix) {
            String value = String.valueOf(rc.getValue());
            if (value.endsWith("*"))
                rc.setValue(value.substring(0, value.length() - 1));
        }

        return rc;
    }

    public static int encodeLong(long value, byte[] buffer, int offset) {
        buffer[offset + 7] = (byte) (value >>> 56);
        buffer[offset + 6] = (byte) (value >>> 48);
        buffer[offset + 5] = (byte) (value >>> 40);
        buffer[offset + 4] = (byte) (value >>> 32);
        buffer[offset + 3] = (byte) (value >>> 24);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 0] = (byte) (value >>> 0);

        return 8;
    }

    public static int encodeFloat(float value, byte[] buffer, int offset) {
        return encodeInteger(Float.floatToRawIntBits(value), buffer, offset);
    }

    public static int encodeInteger(int value, byte[] buffer, int offset) {
        buffer[offset + 3] = (byte) ((value >>> 24) & 0xFF);
        buffer[offset + 2] = (byte) ((value >>> 16) & 0xFF);
        buffer[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        buffer[offset + 0] = (byte) ((value >>> 0) & 0xFF);
        return 4;
    }

    public static int encodeCharacter(char value, byte[] buffer, int offset) {
        buffer[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        buffer[offset + 0] = (byte) ((value >>> 0) & 0xFF);
        return 2;
    }

    public static int decodeInteger(byte[] buffer, int offset) {
        return ((buffer[offset + 3]) << 24) +
                ((buffer[offset + 2] & 0xFF) << 16) +
                ((buffer[offset + 1] & 0xFF) << 8) +
                ((buffer[offset + 0] & 0xFF));
    }

    public static char decodeCharacter(byte[] buffer, int offset) {
        return (char) (((buffer[offset + 1] & 0xFF) << 8) +
                ((buffer[offset + 0] & 0xFF)));
    }


    public static long decodeLong(byte[] buffer, int offset) {
        return (long) buffer[offset] & 0xFF |
                ((long) buffer[offset + 1] << 8L) & 0xFF |
                ((long) buffer[offset + 2] << 16L) & 0xFF |
                ((long) buffer[offset + 3] << 24L) & 0xFF |
                ((long) buffer[offset + 4] << 32L) & 0xFF |
                ((long) buffer[offset + 5] << 40L) & 0xFF |
                ((long) buffer[offset + 6] << 48L) & 0xFF |
                ((long) buffer[offset + 7] << 56L) & 0xFF;
    }

    public static <T> T jsonToObject(String json, Class<T> type) {
        ObjectMapper om = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

        return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            try {
                return om.readValue(json, type);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }


    public static <T> T jsonToObject(StreamInput streamInput, Class<T> type) {
        ObjectMapper om = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

        return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            try {
                return om.readValue(streamInput, type);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static String objectToJson(Object obj) {
        ObjectMapper om = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            try {
                return om.writeValueAsString(obj);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
