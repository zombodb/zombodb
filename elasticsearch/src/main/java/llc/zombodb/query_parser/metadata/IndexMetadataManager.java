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
package llc.zombodb.query_parser.metadata;

import llc.zombodb.query_parser.ASTIndexLink;
import llc.zombodb.query_parser.ASTOptions;
import llc.zombodb.query_parser.QueryParserNode;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;

import java.util.*;

public class IndexMetadataManager {

    static class IndexLinkAndMapping {
        final ASTIndexLink link;
        final GetMappingsResponse mapping;
        final GetSettingsResponse settings;

        private IndexLinkAndMapping(ASTIndexLink link, GetMappingsResponse mapping, GetSettingsResponse settings) {
            this.link = link;
            this.mapping = mapping;
            this.settings = settings;
        }
    }

    private final List<IndexLinkAndMapping> mappings = new ArrayList<>();
    private final Map<String, ASTIndexLink> indexLinksByIndexName = new HashMap<>();
    private List<FieldAndIndexPair> allFields;
    private final IndexRelationshipManager relationshipManager = new IndexRelationshipManager();
    private final Map<ASTIndexLink, IndexMetadata> metadataCache = new HashMap<>();

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
        IndexLinkAndMapping newMe = lookupMapping(myIndex);

        this.myIndex = myIndex;

        // move me to the top of the list so that fields will be
        // resolved starting with me
        mappings.remove(newMe);
        mappings.add(0, newMe);
    }

    private boolean isNestedObjectFieldExternal(String fieldname) {
        for (IndexLinkAndMapping ilap : mappings) {
            if (fieldname.equals(ilap.link.getFieldname()))
                return true;
        }
        return false;
    }

    private ASTIndexLink getExternalIndexLink(String fieldname) {
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

        // this shouldn't every happen
        throw new RuntimeException("Unable to find a mapping for " + link.dumpAsString());
    }

    public IndexMetadata getMetadataForMyIndex() {
        return getMetadata(myIndex);
    }

    public IndexMetadata getMetadataForField(String fieldname) {
        ASTIndexLink fieldSource = findField(fieldname);
        return fieldSource != null ? getMetadata(fieldSource) : null;
    }

    public IndexMetadata getMetadataForIndexName(String indexName) {
        if (mappings == null)
            throw new IllegalArgumentException("No metadata found for: " + indexName);

        for (Map.Entry<ASTIndexLink, IndexMetadata> entry : metadataCache.entrySet()) {
            if (entry.getKey().getIndexName().equalsIgnoreCase(indexName))
                return entry.getValue();
        }
        throw new IllegalArgumentException("No metadata found for: " + indexName);
    }

    private IndexMetadata getMetadata(ASTIndexLink link) {
        if (mappings == null)
            return null;

        try {
            IndexMetadata md = metadataCache.get(link);
            if (md == null)
                metadataCache.put(link, md = new IndexMetadata(link, lookupMapping(link).mapping.getMappings().get(link.getIndexName()).get("data"), Integer.parseInt(lookupMapping(link).settings.getSetting(link.getIndexName(), "index.number_of_shards"))));
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

        GetMappingsResponse mappingResponse = GetMappingsAction.INSTANCE.newRequestBuilder(client)
                .setIndices(indexName)
                .setTypes("data")
                .setIndicesOptions(IndicesOptions.fromOptions(false, false, true, true))
                .get();
        GetSettingsResponse settingsResponse = GetSettingsAction.INSTANCE.newRequestBuilder(client)
                .setIndices(indexName)
                .get();


        if (link == null) {
            String firstIndexName = mappingResponse.getMappings().iterator().next().key;
            String pkey = (String) ((Map) mappingResponse.getMappings().get(firstIndexName).get("data").getSourceAsMap().get("_meta")).get("primary_key");

            String alias = null;
            if (!firstIndexName.equals(indexName)) {
                alias = indexName;
                indexName = firstIndexName;
            }

            link = ASTIndexLink.create(pkey, indexName, alias, pkey, true);
        }

        mappings.add(new IndexMetadataManager.IndexLinkAndMapping(link, mappingResponse, settingsResponse));
        indexLinksByIndexName.put(indexName, link);
        return link;
    }

    public Map<String, ?> describedNestedObject(String fieldname) {
        ASTIndexLink link = findField(fieldname);
        if (link == null)
            return null;

        return getFieldProperties(link, fieldname);
    }

    private Map<String, ?> getFieldProperties(ASTIndexLink link, String fieldname) {
        try {
            if (isNestedObjectFieldExternal(fieldname)) {
                link = getExternalIndexLink(fieldname);
                return lookupMapping(link).mapping.getMappings().get(link.getIndexName()).get("data").getSourceAsMap();
            } else {
                Map properties = (Map) lookupMapping(link).mapping.getMappings().get(link.getIndexName()).get("data").getSourceAsMap().get("properties");
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

        return findLinkForField0(fieldname);
    }

    private ASTIndexLink findLinkForField(String fieldname) {
        if (fieldname.contains(".")) {
            String prefix = fieldname.substring(0, fieldname.indexOf('.'));
            ASTIndexLink link = getExternalIndexLink(prefix);
            fieldname = fieldname.substring(fieldname.indexOf('.') + 1);
            IndexMetadata md = getMetadata(link);
            return (md != null && md.hasField(fieldname)) ? link : myIndex;
        }

        return findLinkForField0(fieldname);
    }

    private ASTIndexLink findLinkForField0(String fieldname) {
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

                if (md.getSearchAnalyzer(field) != null) {
                    FieldAndIndexPair faip = new FieldAndIndexPair(link, field);
                    if (!fields.contains(faip))
                        fields.add(faip);
                }

            }
            if (hasAllField) {
                FieldAndIndexPair faip = new FieldAndIndexPair(link, "_all");
                if (!fields.contains(faip))
                    fields.add(faip);    // we still want the "_all" field if any defined field lives in it
            }
        }
        if (fields.isEmpty())
            fields.add(new FieldAndIndexPair(null, "_all"));

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

    public Stack<Dijkstra.NamedIndex> calculatePath(ASTIndexLink source, ASTIndexLink dest) {
        if (!relationshipManager.relationshipsDefined()) {
            for (IndexLinkAndMapping ilm : mappings) {
                ASTIndexLink link = ilm.link;
                String leftFieldname = link.getLeftFieldname();
                if (leftFieldname.contains("."))
                    leftFieldname = leftFieldname.substring(leftFieldname.indexOf(".") + 1);
                relationshipManager.addRelationship(
                        findLinkForField(link.getLeftFieldname()), leftFieldname,
                        link, link.getRightFieldname()
                );
            }
        }

        Stack<Dijkstra.NamedIndex> stack = new Stack<>();
        List<Dijkstra.NamedIndex> path = relationshipManager.calcPath(source, dest);

        if (path.size() > 1) {
            // trim the top off the path list
            path = path.subList(1, path.size() - 1);

            // reverse it
            Collections.reverse(path);

            // and turn into a stack for use by the caller
            for (Dijkstra.NamedIndex p : path)
                stack.push(p);
        }

        return stack;
    }
}
