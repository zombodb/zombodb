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
package com.tcdi.zombodb.query_parser;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;

import java.util.*;

public class IndexMetadataManager {

    public static class IndexLinkAndMapping {
        public ASTIndexLink link;
        public ActionFuture<GetMappingsResponse> mapping;

        private IndexLinkAndMapping(ASTIndexLink link, ActionFuture<GetMappingsResponse> mapping) {
            this.link = link;
            this.mapping = mapping;
        }
    }

    private final List<IndexLinkAndMapping> mappings = new ArrayList<>();
    private final Map<String, ASTIndexLink> indexLinksByIndexName = new HashMap<>();
    private List<FieldAndIndexPair> allFields;
    private Set<ASTIndexLink> usedIndexes = new HashSet<>();
    private final IndexRelationshipManager relationshipManager = new IndexRelationshipManager();
    private Map<ASTIndexLink, IndexMetadata> metadataCache = new HashMap<>();

    private final Client client;
    private ASTIndexLink myIndex;

    public IndexMetadataManager(Client client, String indexName) {
        this.client = client;
        myIndex = loadMapping(indexName, null);
    }

    public ASTIndexLink getMyIndex() {
        return myIndex;
    }

    public void setMyIndex(ASTIndexLink myIndex) {
        this.myIndex = myIndex;
    }

    public Set<ASTIndexLink> getUsedIndexes() {
        return usedIndexes;
    }

    public void setUsedIndexes(Set<ASTIndexLink> usedIndexes) {
        this.usedIndexes = usedIndexes;
    }

    private boolean isNestedObjectFieldExternal(String fieldname) {
        for (IndexLinkAndMapping ilap : mappings) {
            if (fieldname.equals(ilap.link.getFieldname()))
                return true;
        }
        return false;
    }

    public ASTIndexLink getExternalIndexLink(String fieldname) {
        for (IndexLinkAndMapping ilap : mappings) {
            if (fieldname.equals(ilap.link.getFieldname()))
                return ilap.link;
        }
        throw new RuntimeException(fieldname + " is not external");
    }

    public ASTIndexLink getIndexLinkByIndexName(String indexName) {
        return indexLinksByIndexName.get(indexName);
    }

    private IndexMetadataManager.IndexLinkAndMapping lookupMapping(ASTIndexLink link) {
        for (IndexMetadataManager.IndexLinkAndMapping ilam : mappings)
            if (link == ilam.link)
                return ilam;
        return null;
    }

    public IndexMetadata getMetadataForMyIndex() {
        return getMetadata(myIndex);
    }

    public IndexMetadata getMetadataForField(String fieldname) {
        ASTIndexLink fieldSource = findField(fieldname);
        return fieldSource != null ? getMetadata(fieldSource) : null;
    }

    public IndexMetadata getMetadata(ASTIndexLink link) {
        if (mappings == null)
            return null;

        try {
            IndexMetadata md = metadataCache.get(link);
            if (md == null)
                metadataCache.put(link, md = new IndexMetadata(link, lookupMapping(link).mapping.get().getMappings().get(link.getIndexName()).get("data")));
            return md;
        } catch (NullPointerException npe) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ASTIndexLink loadMapping(String indexName, ASTIndexLink link) {
        if (client == null)
            return link; // nothing we can do

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indexName).types("data");
        getMappingsRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, true));
        getMappingsRequest.local(false);

        ActionFuture<GetMappingsResponse> future = client.admin().indices().getMappings(getMappingsRequest);
        if (link == null) {
            try {
                GetMappingsResponse response = future.get();
                String pkey = (String) ((Map) response.getMappings().get(indexName).get("data").getSourceAsMap().get("_meta")).get("primary_key");
                link = ASTIndexLink.create(pkey, indexName, pkey, true);
            } catch (Exception e) {
                throw new RuntimeException("Problem creating anonymous ASTIndexLink for " + indexName, e);
            }
        }

        mappings.add(new IndexMetadataManager.IndexLinkAndMapping(link, future));
        indexLinksByIndexName.put(indexName, link);
        return link;
    }

    public Map<String, ?> describedNestedObject(String fieldname) throws Exception {
        ASTIndexLink link = findField(fieldname);
        if (link == null)
            return null;

        return getFieldProperties(link, fieldname);
    }

    private Map<String, ?> getFieldProperties(ASTIndexLink link, String fieldname) {
        try {
            if (isNestedObjectFieldExternal(fieldname)) {
                link = getExternalIndexLink(fieldname);
                return (Map) lookupMapping(link).mapping.get().getMappings().get(link.getIndexName()).get("data").getSourceAsMap();
            } else {
                Map properties = (Map) lookupMapping(link).mapping.get().getMappings().get(link.getIndexName()).get("data").getSourceAsMap().get("properties");
                return (Map<String, ?>) properties.get(fieldname);
            }
        } catch (NullPointerException npe) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ASTIndexLink findField(String fieldname) {
        if (fieldname == null)
            return myIndex;

        return findField0(fieldname);
    }

    private ASTIndexLink findField0(String fieldname) {
        if (fieldname.contains("."))
            fieldname = fieldname.substring(0, fieldname.indexOf('.'));

        for (IndexMetadataManager.IndexLinkAndMapping ilam : mappings) {
            ASTIndexLink link = ilam.link;

            if (fieldname.equals(link.getFieldname()))
                return link;

            IndexMetadata md = getMetadata(link);
            if (md != null && md.hasField(fieldname))
                return link;
        }
        return myIndex;
    }

    private ASTIndexLink findFieldForLink(String fieldname) {
        if (fieldname.contains(".")) {
            String prefix = fieldname.substring(0, fieldname.indexOf('.'));
            ASTIndexLink link = getExternalIndexLink(prefix);
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);
            IndexMetadata md = getMetadata(link);
            return (md != null && md.hasField(fieldname)) ? link : myIndex;
        }

        for (IndexMetadataManager.IndexLinkAndMapping ilam : mappings) {
            ASTIndexLink link = ilam.link;

            if (fieldname.equals(link.getFieldname()))
                return link;

            IndexMetadata md = getMetadata(link);
            if (md != null && md.hasField(fieldname))
                return link;
        }
        return myIndex;
    }

    public List<FieldAndIndexPair> resolveAllField() {
        if (allFields != null)
            return allFields;

        List<FieldAndIndexPair> fields = new ArrayList<>(200);
        for (IndexMetadataManager.IndexLinkAndMapping ilam : mappings) {
            ASTIndexLink link = ilam.link;
            IndexMetadata md = getMetadata(link);

            if (md == null)
                continue;

            boolean hasAllField = false;
            for (String field : md.getFields()) {
                if (md.getIncludeInAll(field)) {
                    hasAllField = true;
                    continue;
                }

                if (md.getSearchAnalyzer(field) != null)
                    fields.add(new FieldAndIndexPair(link, field, md));

            }
            if (hasAllField)
                fields.add(new FieldAndIndexPair(link, "_all", md));    // we still want the "_all" field if any defined field lives in it
        }
        if (fields.isEmpty())
            fields.add(new FieldAndIndexPair(null, "_all", null));

        return allFields = fields;
    }

    public void loadReferencedMappings(ASTOptions options) {
        if (options == null)
            return;

        for (QueryParserNode node : options) {
            if (node instanceof ASTIndexLink) {
                ASTIndexLink link = (ASTIndexLink) node;
                String indexName = link.getIndexName();

                if (indexName.split("[.]").length > 2) {
                    // (hopefully) already fully qualified
                    loadMapping(link.getIndexName(), link);
                } else if (myIndex != null) {
                    // not fully qualified, so we need to do that (if we know our index)
                    String prefix = myIndex.getIndexName();
                    prefix = prefix.substring(0, prefix.lastIndexOf('.'));    // strip off current index name
                    prefix = prefix.substring(0, prefix.lastIndexOf('.'));    // strip off current table name

                    // fully qualify the index name
                    link.qualifyIndexName(prefix);
                    loadMapping(link.getIndexName(), link);
                }
            }
        }
    }

    public Stack<String> calculatePath(ASTIndexLink source, ASTIndexLink dest) {
        if (!relationshipManager.relationshipsDefined()) {
            for (IndexLinkAndMapping ilm : mappings) {
                ASTIndexLink link = ilm.link;
                String leftFieldname = link.getLeftFieldname();
                if (leftFieldname.contains("."))
                    leftFieldname = leftFieldname.substring(leftFieldname.indexOf(".") + 1);
                relationshipManager.addRelationship(findFieldForLink(link.getLeftFieldname()).getIndexName(), leftFieldname, link.getIndexName(), link.getRightFieldname());
            }
        }

        Stack<String> stack = new Stack<>();
        List<String> path = relationshipManager.calcPath(source.getIndexName(), dest.getIndexName());

        if (path.size() > 1) {
            // trim the top off the path list
            path = path.subList(1, path.size() - 1);

            // reverse it
            Collections.reverse(path);

            // and turn into a stack for use by the caller
            for (String p : path)
                stack.push(p);
        }

        return stack;
    }

    public boolean areFieldPathsEquivalent(String a, String b) {
        return relationshipManager.areFieldsEquivalent(a, b);
    }

}
