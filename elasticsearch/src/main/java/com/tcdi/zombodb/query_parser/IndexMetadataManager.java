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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.IdentityHashSet;

import java.util.*;

/**
 * Created by e_ridge on 2/11/15.
 */
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
    private Set<ASTIndexLink> usedIndexes = new IdentityHashSet<>();
    private final IndexRelationshipManager relationshipManager = new IndexRelationshipManager();
    private Map<ASTIndexLink, IndexMetadata> metadataCache = new HashMap<>();

    private final Client client;
    private final ASTIndexLink originalMyIndex;
    private ASTIndexLink myIndex;

    public IndexMetadataManager(Client client, ASTIndexLink myIndex) {
        this.client = client;
        this.myIndex = originalMyIndex = myIndex;
        loadMapping(myIndex);
    }

    public ASTIndexLink getOriginalMyIndex() {
        return originalMyIndex;
    }

    public ASTIndexLink getMyIndex() {
        return myIndex;
    }

    public void setMyIndex(ASTIndexLink myIndex) {
        this.myIndex = myIndex;
    }

    public boolean isMyIndexSwapped() {
        return myIndex != originalMyIndex;
    }

    public Set<ASTIndexLink> getUsedIndexes() {
        return usedIndexes;
    }

    public void setUsedIndexes(Set<ASTIndexLink> usedIndexes) {
        this.usedIndexes = usedIndexes;
    }

    public Collection<IndexLinkAndMapping> getAllMappings() {
        return mappings;
    }

    private boolean isNestedObjectFieldExternal(String fieldname) {
        for (IndexLinkAndMapping ilap : mappings) {
            if (fieldname.equals(ilap.link.getFieldname()))
                return true;
        }
        return false;
    }

    public boolean isFieldNested(String fieldname) {
        if (!fieldname.contains("."))
            return false;

        IndexMetadata md = getMetadataForField(fieldname);
        String base = fieldname.substring(0, fieldname.indexOf("."));
        return !base.equals(md.getLink().getFieldname()) || (base.equals(md.getLink().getFieldname()) && fieldname.substring(fieldname.indexOf(".")+1).contains("."));
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

    public IndexMetadata getMetadataForMyOriginalIndex() {
        return getMetadata(originalMyIndex);
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

    public IndexMetadata getMetadata(String indexName) {
        for (IndexLinkAndMapping ilam : mappings) {
            if (ilam.link.getIndexName().equals(indexName))
                return getMetadata(ilam.link);
        }
        return null;
    }

    private void loadMapping(ASTIndexLink link) {
        if (client == null)
            return; // nothing we can do

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(link.getIndexName()).types("data");
        getMappingsRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, true));
        getMappingsRequest.local(false);
        mappings.add(new IndexMetadataManager.IndexLinkAndMapping(link, client.admin().indices().getMappings(getMappingsRequest)));
        indexLinksByIndexName.put(link.getIndexName(), link);
    }

    public Map<String, ?> describedNestedObject(String fieldname) throws Exception {
        ASTIndexLink link = findField(fieldname);
        if (link == null)
            return null;
        Map<String, ?> properties = getFieldProperties(link, fieldname);

        return properties;
    }

    private Map<String, ?> getFieldProperties(ASTIndexLink link, String fieldname) {
        try {
            if (isNestedObjectFieldExternal(fieldname)) {
                link = getExternalIndexLink(fieldname);
                Map properties = (Map) lookupMapping(link).mapping.get().getMappings().get(link.getIndexName()).get("data").getSourceAsMap();
                return properties;
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

    public boolean isFieldElsewhere(String fieldname) {
        if (fieldname == null || Arrays.binarySearch(IndexMetadata.IGNORED_FIELDS, fieldname) > -1)
            return false;

        for (IndexMetadataManager.IndexLinkAndMapping ilam : mappings) {
            ASTIndexLink link = ilam.link;

            IndexMetadata md = getMetadata(link);
            if (md != null && md.hasField(fieldname)) {
                return link != myIndex;
            }
        }
        return false;
    }

    public ASTIndexLink findField(String fieldname) {
        if (fieldname == null)
            return myIndex;

        return findField0(fieldname);
    }

    private ASTIndexLink findField0(String fieldname) {
        if (fieldname.contains("."))
            fieldname = fieldname.substring(0, fieldname.indexOf('.'));

        if (Arrays.binarySearch(IndexMetadata.IGNORED_FIELDS, fieldname) > -1)
            return myIndex;

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
                    loadMapping(link);
                } else if (myIndex != null) {
                    // not fully qualified, so we need to do that (if we know our index)
                    String prefix = myIndex.getIndexName();
                    prefix = prefix.substring(0, prefix.lastIndexOf('.'));    // strip off current index name
                    prefix = prefix.substring(0, prefix.lastIndexOf('.'));    // strip off current table name

                    // fully qualify the index name
                    link.qualifyIndexName(prefix);
                    loadMapping(link);
                }
            }
        }
    }

    public List<String> calculatePath(ASTIndexLink source, ASTIndexLink dest) {
        if (source.getIndexName().equals(dest.getIndexName())) {
            // short cut if the source and destination indexes are the same
            // the path is just the left and right fields of the source
            return Arrays.asList(source.getIndexName()+":"+source.getLeftFieldname(), source.getIndexName()+":"+source.getRightFieldname());
        }

        if (!relationshipManager.relationshipsDefined()) {
            for (IndexLinkAndMapping ilm : mappings) {
                ASTIndexLink link = ilm.link;
                relationshipManager.addRelationship(findField(link.getLeftFieldname()).getIndexName(), link.getLeftFieldname(), link.getIndexName(), link.getRightFieldname());
            }
        }

        List<String> path = relationshipManager.calcPath(source.getIndexName(), dest.getIndexName());
        return path.subList(1, path.size()-1);
    }

    public boolean areFieldPathsEquivalent(String a, String b) {
        return relationshipManager.areFieldsEquivalent(a, b);
    }

}
