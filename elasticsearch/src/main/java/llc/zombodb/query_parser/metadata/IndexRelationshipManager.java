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
package llc.zombodb.query_parser.metadata;

import llc.zombodb.query_parser.ASTIndexLink;

import java.util.*;

public class IndexRelationshipManager {

    private final Dijkstra d = new Dijkstra();

    private int cnt = 0;
    private final Map<String, Set<String>> equivalencies = new HashMap<>();

    public IndexRelationshipManager() {

    }

    boolean relationshipsDefined() {
        return cnt > 0;
    }

    public void addRelationship(ASTIndexLink source, String sourceField, ASTIndexLink destination, String destinationField) {
        addRelationship(source.getFieldname(), source.getIndexName(), sourceField, destination.getFieldname(), destination.getIndexName(), destinationField);
    }

    private void addRelationship(String sourceName, String sourceIndex, String sourceField, String destinationName, String destinationIndex, String destinationField) {
        Dijkstra.Vertex source = d.vertex(sourceName, sourceIndex);
        Dijkstra.Vertex dest = d.vertex(destinationName, destinationIndex);

        Dijkstra.Vertex srcField = d.vertex(sourceName, sourceIndex + ":" + sourceField);
        Dijkstra.Vertex destField = d.vertex(destinationName, destinationIndex + ":" + destinationField);

        // source links to dest via the source and dest fields
        source.add(srcField, 1).add(destField, 1).add(dest, 2);
        // dest links to source via the dest and source fields
        dest.add(destField, 1).add(srcField, 1).add(source, 2);

        addEquivalency(srcField, destField);
        addEquivalency(destField, srcField);
        cnt++;
    }

    public List<Dijkstra.NamedIndex> calcPath(ASTIndexLink source, ASTIndexLink destination) {
        return calcPath(source.getFieldname(), source.getIndexName(), destination.getFieldname(), destination.getIndexName());
    }

    private List<Dijkstra.NamedIndex> calcPath(String sourceName, String sourceIndex, String destinationName, String destinationIndex) {
        List<Dijkstra.Vertex> path = d.getShortestPathTo(sourceName, sourceIndex, destinationName, destinationIndex);
        List<Dijkstra.NamedIndex> pathAsStrings = new ArrayList<>(path.size());
        for (Dijkstra.Vertex p : path)
            pathAsStrings.add(p.getName());
        return pathAsStrings;
    }

    private void addEquivalency(Dijkstra.Vertex srcField, Dijkstra.Vertex destField) {
        Set<String> equiv;

        equiv = equivalencies.get(srcField.toString());
        if (equiv == null)
            equivalencies.put(srcField.toString(), equiv = new HashSet<>());
        equiv.add(destField.toString());
    }

}
