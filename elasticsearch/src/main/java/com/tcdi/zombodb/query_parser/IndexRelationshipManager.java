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

import java.util.*;

public class IndexRelationshipManager {

    private Dijkstra d = new Dijkstra();

    private int cnt = 0;
    private Map<String, Set<String>> equivalencies = new HashMap<>();

    public IndexRelationshipManager() {

    }

    public boolean relationshipsDefined() {
        return cnt > 0;
    }

    public void addRelationship(String sourceIndex, String sourceField, String destinationIndex, String destinationField) {
        Dijkstra.Vertex source = d.vertex(sourceIndex);
        Dijkstra.Vertex dest = d.vertex(destinationIndex);

        Dijkstra.Vertex srcField = d.vertex(sourceIndex + ":" + sourceField);
        Dijkstra.Vertex destField = d.vertex(destinationIndex + ":" + destinationField);

        // source links to dest via the source and dest fields
        source.add(srcField, 1).add(destField, 1).add(dest, 2);
        // dest links to source via the dest and source fields
        dest.add(destField, 1).add(srcField, 1).add(source, 2);

        addEquivalency(srcField, destField);
        addEquivalency(destField, srcField);
        cnt++;
    }

    public List<String> calcPath(String sourceIndex, String destinationIndex) {
        List<Dijkstra.Vertex> path = d.getShortestPathTo(sourceIndex, destinationIndex);
        List<String> pathAsStrings = new ArrayList<>(path.size());
        for (Dijkstra.Vertex p : path)
            pathAsStrings.add(p.toString());
        return pathAsStrings;
    }

    public boolean areFieldsEquivalent(String a, String b) {
        Set<String> equiv = equivalencies.get(a);
        return equiv != null && equiv.contains(b);
    }

    private void addEquivalency(Dijkstra.Vertex srcField, Dijkstra.Vertex destField) {
        Set<String> equiv;

        equiv = equivalencies.get(srcField.toString());
        if (equiv == null)
            equivalencies.put(srcField.toString(), equiv = new HashSet<>());
        equiv.add(destField.toString());
    }

}
