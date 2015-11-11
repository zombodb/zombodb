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

import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
* Created by e_ridge on 2/11/15.
*/
class IndexMetadata {
    static final String[] IGNORED_FIELDS = new String[] {
            "_xmin",
            "_xmax",
            "_cmin",
            "_cmax",
            "_xmin_is_committed",
            "_xmax_is_committed",
            "_partial"
    };
    static {
        Arrays.sort(IGNORED_FIELDS);
    }

    static final String[] MLT_STOP_WORDS = new String[] {
            "http", "span", "class", "flashtext", "let", "its",
            "may", "well", "got", "too", "them", "really", "new", "set", "please",
            "how", "our", "from", "sent", "subject", "sincerely", "thank", "thanks",
            "just", "get", "going", "were", "much", "can", "also", "she", "her" ,
            "him", "his", "has", "been", "ok", "still", "okay", "does", "did",
            "about", "yes", "you", "your", "when", "know", "have", "who", "what",
            "where", "sir", "page", "a", "an", "and", "are", "as", "at", "be",
            "but", "by","for", "if", "in", "into", "is", "it","no", "not", "of",
            "on", "or", "such","that", "the", "their", "than", "then", "there",
            "these","they", "this", "to", "was", "will", "with"
    };
    static final char[] NEEDS_ESCAPES = new char[] { 'A', 'a', 'O', 'o', '\t', '\n', '\r', '\f', '$', '^', '/', ':', '=', '<', '>', '!', '#', '@', '(', ')', '"', '\'', '.', ',', '&', '[', ']' };
    static final String NEEDS_ESCAPES_AS_STRING;
    static {
        Arrays.sort(IndexMetadata.NEEDS_ESCAPES);

        StringBuilder sb = new StringBuilder();
        for (char ch : IndexMetadata.NEEDS_ESCAPES) {
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

    private final ASTIndexLink link;


    private Map<String, Map<String, Object>> fields;
    private String pkeyFieldName;
    private boolean noxact;

    public IndexMetadata(ASTIndexLink link, MappingMetaData mmd) {
        this.link = link;
        try {
            Map meta = (Map) mmd.getSourceAsMap().get("_meta");

            fields = (Map) mmd.getSourceAsMap().get("properties");
            fields.put("_all", (Map) mmd.getSourceAsMap().get("_all"));
            pkeyFieldName = meta != null ? (String) meta.get("primary_key") : null;
            Boolean noxact = meta != null ? (Boolean) meta.get("noxact") : null;
            this.noxact = noxact != null ? noxact : false;
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

    public boolean getNoXact() {
        return noxact;
    }

    public boolean hasField(String fieldname) {
        if (fieldname.indexOf('.') < 0)
            return fields.containsKey(fieldname);

        Map fields = this.fields;
        while (fieldname.contains(".")) {
            String base = fieldname.substring(0, fieldname.indexOf('.'));
            fieldname = fieldname.substring(fieldname.indexOf('.')+1);

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

    public String getType(String fieldname) {
        String[] parts = fieldname.split("[.]");
        if (parts.length == 1) {
            Map<String, Object> fieldProperties = fields.get(fieldname);
            if (fieldProperties == null) {
                return "unknown";
            }
            return String.valueOf(fieldProperties.get("type"));
        }

        Map properties = fields;
        for (int i=0; properties != null && i<parts.length; i++) {
            properties = ((Map) properties.get(parts[i]));
            if (properties != null && i<parts.length-1)
                properties = (Map) properties.get("properties");
        }

        if (properties == null)
            return "unknown";

        return String.valueOf(properties.get("type"));
    }

    public boolean getIncludeInAll(String fieldname) {
        Object o = fields.get(fieldname).get("include_in_all");
        return o == null || o == Boolean.TRUE || "true".equalsIgnoreCase(String.valueOf(o));
    }

    public String getAnalyzer(String fieldname) {
        Map<String, Object> fieldInfo = fields.get(fieldname);
        Object analyzer = fieldInfo == null ? null : fieldInfo.get("analyzer");
        return analyzer == null ? null : String.valueOf(analyzer);
    }

    public boolean canUseFieldData(String fieldname) {
        Map<String, Object> fieldInfo = fields.get(fieldname);
        if (fieldInfo == null)
            return false;
        Map<String, Object> fielddata = (Map) fieldInfo.get("fielddata");
        if (fielddata == null)
            return true;

        return !"disabled".equals(fielddata.get("format"));
    }

    public Set<String> getFields() {
        return fields.keySet();
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}
