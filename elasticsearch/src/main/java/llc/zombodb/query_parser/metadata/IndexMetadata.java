/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2017 ZomboDB, LLC
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
package llc.zombodb.query_parser.metadata;

import llc.zombodb.query_parser.ASTIndexLink;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IndexMetadata {

    public static final String[] MLT_STOP_WORDS = new String[]{
            "http", "span", "class", "flashtext", "let", "its",
            "may", "well", "got", "too", "them", "really", "new", "set", "please",
            "how", "our", "from", "sent", "subject", "sincerely", "thank", "thanks",
            "just", "get", "going", "were", "much", "can", "also", "she", "her",
            "him", "his", "has", "been", "ok", "still", "okay", "does", "did",
            "about", "yes", "you", "your", "when", "know", "have", "who", "what",
            "where", "sir", "page", "a", "an", "and", "are", "as", "at", "be",
            "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of",
            "on", "or", "such", "that", "the", "their", "than", "then", "there",
            "these", "they", "this", "to", "was", "will", "with"
    };

    private final ASTIndexLink link;
    private final int numberOfShards;


    private final Map<String, Map<String, Object>> fields;
    private final String pkeyFieldName;
    private boolean alwaysResolveJoins = false;
    private String blockRoutingField;

    public IndexMetadata(ASTIndexLink link, MappingMetaData mmd, int numberOfShards) {
        this.link = link;
        this.numberOfShards = numberOfShards;
        Map meta = (Map) mmd.getSourceAsMap().get("_meta");

        fields = (Map) mmd.getSourceAsMap().get("properties");
        fields.put("_all", (Map) mmd.getSourceAsMap().get("_all"));
        pullUpMultiFields();

        pkeyFieldName = meta != null ? (String) meta.get("primary_key") : null;
        alwaysResolveJoins = meta != null && (meta.containsKey("always_resolve_joins") && "true".equals(String.valueOf(meta.get("always_resolve_joins"))));
        blockRoutingField = meta != null ? (String) meta.get("block_routing_field") : null;
        if ("null".equals(blockRoutingField))
            blockRoutingField = null;
    }

    public ASTIndexLink getLink() {
        return link;
    }

    public String getPrimaryKeyFieldName() {
        return pkeyFieldName;
    }

    public String getBlockRoutingField() {
        return blockRoutingField;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public boolean alwaysResolveJoins() {
        return alwaysResolveJoins;
    }

    public boolean isMultiField(String fieldname) {
        return fieldname != null && fieldname.contains(".") && fields.containsKey(fieldname);
    }

    public boolean hasField(String fieldname) {
        if (fieldname == null)
            fieldname = "_all";

        return fields.containsKey(fieldname) || isMultiField(fieldname) || (isNested(fieldname) && containsNested(fieldname));
    }

    private boolean containsNested(String fieldname) {
        if (fieldname == null)
            return false;
        if (!fieldname.contains("."))
            return false;
        if (isMultiField(fieldname))
            return false;

        Map fields = this.fields;
        while (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);

            Object value = fields.get(base);
            if (value == null)
                return false;
            else if (value instanceof Map) {
                fields = (Map) ((Map) value).get("properties");
                if (fields == null)
                    return false;
            }
        }

        return true;
    }

    public boolean isNested(String fieldname) {
        if (fieldname == null)
            return false;
        if (!fieldname.contains("."))
            return false;
        if (isMultiField(fieldname))
            return false;

        Map fields = this.fields;
        while (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);

            Object value = fields.get(base);
            if (value == null)
                return false;
            else if (value instanceof Map) {
                if ("nested".equals(((Map) value).get("type")))
                    return true;

                fields = (Map) ((Map) value).get("properties");
                if (fields == null)
                    return false;
            }
        }

        return false;
    }

    public String getMaximalNestedPath(String fieldname) {
        if (!isNested(fieldname))
            return null;

        String path = null;
        Map parent = null;
        Map fields = this.fields;
        while (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);

            Object value = fields.get(base);
            if (value == null)
                return path;
            else if (value instanceof Map) {
                parent = (Map) value;

                if (!"nested".equals(parent.get("type")))
                    return path;

                if (path == null)
                    path = base;
                else
                    path += "." + base;

                fields = (Map) ((Map) value).get("properties");
                if (fields == null)
                    return path;
            }
        }

        return path;
    }

    public boolean isNestedObjectField(String fieldname) {
        return "nested".equals(getType(fieldname));
    }

    public String getNestedObjectSentinelField(String fieldname) {
        return fieldname + ".zdb_always_exists";
    }

    public String getNullValue(String fieldname) {
        return getFieldProperty(fieldname, "null_value");
    }

    public String getType(String fieldname) {
        if ("_all".equals(fieldname))
            return "string";
        String type = getFieldProperty(fieldname, "type");
        if ("text".equals(type) || "keyword".equals(type))
            return "string";
        return type == null ? "unknown" : type;
    }

    private <T> T getFieldProperty(String fieldname, String property) {
        if (fieldname == null)
            fieldname = "_all";

        Map fields = this.fields;
        if (fieldname.contains(".")) {
            if (isMultiField(fieldname)) {
                fields = getNestedOrMultifieldPropertyMapContainer(fields, fieldname, "fields");
            } else {
                fields = getNestedOrMultifieldPropertyMapContainer(fields, fieldname, "properties");
            }

            if (fields == null) {
                // couldn't find it above, so try it within us
                // but with just the final fieldname
                fields = this.fields;
                fields = (Map) fields.get(fieldname.substring(fieldname.lastIndexOf('.') + 1));
            }

        } else {
            fields = (Map) fields.get(fieldname);
        }

        return fields != null ? (T) fields.get(property) : null;
    }

    private Map getNestedOrMultifieldPropertyMapContainer(Map fields, String fieldname, String fieldCompositionType) {

        while (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);

            Object value = fields.get(base);
            if (value == null)
                return null;
            else if (value instanceof Map) {
                fields = (Map) ((Map) value).get(fieldCompositionType);
                if (fields == null)
                    return null;
            }
        }

        return (Map) fields.get(fieldname);
    }

    public boolean getIncludeInAll(String fieldname) {
        Boolean includeInAll = getFieldProperty(fieldname, "include_in_all");
        return includeInAll == null || // because "include_in_all" is our index default
                includeInAll;   // or it might be explicitly set to true
    }

    public String getSearchAnalyzer(String fieldname) {
        return getAnalyzerOfType(fieldname, "search");
    }

    public String getIndexAnalyzer(String fieldname) {
        return getAnalyzerOfType(fieldname, "index");
    }

    private String getAnalyzerOfType(String fieldname, String analyzerType) {
        if (fieldname == null)
            fieldname = "_all";

        if (!hasField(fieldname))
            return "exact";

        String analyzer;

        analyzer = getFieldProperty(fieldname, analyzerType + "_analyzer");
        if (analyzer == null)
            analyzer = getFieldProperty(fieldname, "analyzer");
        if (analyzer == null)
            analyzer = getFieldProperty(fieldname, "normalizer");

        return analyzer;
    }

    public Set<String> getFields() {
        return fields.keySet();
    }

    @Override
    public String toString() {
        return fields.toString();
    }

    private void pullUpMultiFields() {
        Map<String, Map<String, Object>> found = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : fields.entrySet()) {
            Map<String, Object> multifields = (Map) entry.getValue().get("fields");
            if (multifields != null) {
                for (Map.Entry<String, Object> mfield : multifields.entrySet()) {
                    found.put(entry.getKey() + "." + mfield.getKey(), (Map) mfield.getValue());
                }
            }
        }
        fields.putAll(found);
    }

    private String valueOf(Object o) {
        return o == null ? null : String.valueOf(o);
    }

}
