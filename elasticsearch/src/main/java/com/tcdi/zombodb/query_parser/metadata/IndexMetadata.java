/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2016 ZomboDB, LLC
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
package com.tcdi.zombodb.query_parser.metadata;

import com.tcdi.zombodb.query_parser.ASTIndexLink;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
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


    private Map<String, Map<String, Object>> fields;
    private String pkeyFieldName;
    private boolean alwaysResolveJoins = false;

    public IndexMetadata(ASTIndexLink link, MappingMetaData mmd) {
        this.link = link;
        try {
            Map meta = (Map) mmd.getSourceAsMap().get("_meta");

            fields = (Map) mmd.getSourceAsMap().get("properties");
            fields.put("_all", (Map) mmd.getSourceAsMap().get("_all"));
            pullUpMultiFields();

            pkeyFieldName = meta != null ? (String) meta.get("primary_key") : null;
            alwaysResolveJoins = meta.containsKey("always_resolve_joins") && "true".equals(String.valueOf(meta.get("always_resolve_joins")));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public ASTIndexLink getLink() {
        return link;
    }

    public String getPrimaryKeyFieldName() {
        return pkeyFieldName;
    }

    public boolean alwaysResolveJoins() {
        return alwaysResolveJoins;
    }

    public boolean isMultiField(String fieldname) {
        return fieldname != null && fieldname.contains(".") && fields.containsKey(fieldname);
    }

    public boolean hasField(String fieldname) {
        if (isMultiField(fieldname))
            return true;

        Map fields = this.fields;
        while (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);

            Object value = fields.get(base);
            if (value == null)
                return false;
            else if (value instanceof Map) {
                fields = (Map) ((Map) value).get("fields");
                if (fields == null)
                    return false;
            }
        }

        return fields.containsKey(fieldname);
    }

    public String getNullValue(String fieldname) {
        return getFieldProperty(fieldname, "null_value");
    }

    public String getType(String fieldname) {
        if ("_all".equals(fieldname))
            return "string";
        String type = getFieldProperty(fieldname, "type");
        return type == null ? "unknown" : type;
    }

    public String getFieldProperty(String fieldname, String property) {
        if (fieldname == null)
            return null;

        if (isMultiField(fieldname)) {
            Map properties = (Map) fields.get(fieldname).get("properties");
            if (properties != null)
                return valueOf(properties.get(property));
        }

        String[] parts = fieldname.split("[.]");
        if (parts.length == 1) {
            Map<String, Object> fieldProperties = fields.get(fieldname);
            if (fieldProperties == null) {
                return null;
            }
            return valueOf(fieldProperties.get(property));
        }

        Map properties = fields;
        for (int i = 0; properties != null && i < parts.length; i++) {
            properties = ((Map) properties.get(parts[i]));
            if (properties != null && i < parts.length - 1)
                properties = (Map) properties.get("properties");
        }

        if (properties == null)
            return null;

        return valueOf(properties.get(property));
    }

    public boolean getIncludeInAll(String fieldname) {
//        Object o = fields.get(fieldname).get("include_in_all");
//        return o == null || o == Boolean.TRUE || "true".equalsIgnoreCase(String.valueOf(o));
        String includeInAll = getFieldProperty(fieldname, "include_in_all");
        return includeInAll == null || "true".equalsIgnoreCase(includeInAll);
    }

    public String getSearchAnalyzer(String fieldname) {
        Map<String, Object> fieldInfo = fields.get(fieldname);
        if (fieldInfo == null)
            return "exact"; // we don't know about this field, so assume it's using index default analyzer of 'exact'

        String analyzer = (String) fieldInfo.get("search_analyzer");
        if (analyzer == null)
            analyzer = (String) fieldInfo.get("analyzer");

        return analyzer;
    }

    public String getIndexAnalyzer(String fieldname) {
        Map<String, Object> fieldInfo = fields.get(fieldname);
        if (fieldInfo == null)
            return "exact"; // we don't know about this field, so assume it's using index default analyzer of 'exact'

        String analyzer = (String) fieldInfo.get("index_analyzer");
        if (analyzer == null)
            analyzer = (String) fieldInfo.get("analyzer");

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
        for(Map.Entry<String, Map<String, Object>> entry : fields.entrySet()) {
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
