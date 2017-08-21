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
package com.tcdi.zombodb.query_parser.metadata;

import java.util.*;


/**
 * Adapted from http://www.algolist.com/code/java/Dijkstra's_algorithm.
 * <p/>
 * See also:  http://en.wikipedia.org/wiki/Dijkstra's_algorithm
 */
public class Dijkstra {
    public class Vertex implements Comparable<Vertex>, Iterable<Edge> {
        private final NamedIndex name;
        private final List<Edge> adjacencies = new ArrayList<>();
        private double minDistance = Double.POSITIVE_INFINITY;
        private Vertex previous;

        private Vertex(NamedIndex argName) {
            name = argName;
        }

        public NamedIndex getName() {
            return name;
        }

        public String toString() {
            return name.toString();
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Vertex && name.equals(((Vertex) obj).name);
        }

        public int compareTo(Vertex other) {
            return Double.compare(minDistance, other.minDistance);
        }

        @Override
        public Iterator<Edge> iterator() {
            return new ArrayList<>(adjacencies).iterator();
        }

        public Vertex add(String name, String index, double weight) {
            Edge e;
            add(e = edge(name, index, weight));
            return e.target;
        }

        public Vertex add(Vertex v, double weight) {
            add(new Edge(v, weight));
            return v;
        }

        public Vertex add(Edge e) {
            if (name.equals(e.target.name))
                return this;
            else {
                for (Edge edge : adjacencies)
                    if (edge.target.equals(e.target))
                        return this;
            }
            adjacencies.add(e);
            return e.target;
        }

        private void reset() {
            minDistance = Double.POSITIVE_INFINITY;
            previous = null;
            for (Edge e : adjacencies) {
                e.reset();
            }
        }
    }

    class Edge {
        final Vertex target;
        final double weight;

        private Edge(Vertex argTarget, double argWeight) {
            target = argTarget;
            weight = argWeight;
        }

        private void reset() {
            if (target.previous != null)
                target.reset();
        }
    }

    public class NamedIndex {
        public final String name;
        public final String index;

        NamedIndex(String name, String index) {
            this.name = name;
            this.index = index;
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public String toString() {
            return name + "@" + index;
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && obj instanceof NamedIndex && obj.toString().equals(this.toString());
        }
    }

    private final Map<NamedIndex, Vertex> verticies = new HashMap<>();

    Vertex vertex(String name, String index) {
        NamedIndex ni = new NamedIndex(name, index);
        Vertex v = verticies.get(ni);
        if (v == null)
            verticies.put(ni, v = new Vertex(ni));
        return v;
    }

    private Edge edge(String name, String index, double weight) {
        return new Edge(vertex(name, index), weight);
    }

    private void computePaths(Vertex source) {
        for (Vertex v : verticies.values())
            v.reset();

        PriorityQueue<Vertex> vertexQueue = new PriorityQueue<>();
        vertexQueue.add(source);

        source.minDistance = 0;


        while (!vertexQueue.isEmpty()) {
            Vertex u = vertexQueue.poll();

            for (Edge e : u.adjacencies) {
                Vertex v = e.target;

                double weight = e.weight;
                double distanceThroughU = u.minDistance + weight;
                if (distanceThroughU < v.minDistance) {
                    vertexQueue.remove(v);
                    v.minDistance = distanceThroughU;
                    v.previous = u;
                    vertexQueue.add(v);
                }
            }
        }
    }

    List<Vertex> getShortestPathTo(String sourceName, String sourceIndex, String destinationName, String destinationIndex) {
        NamedIndex source = new NamedIndex(sourceName, sourceIndex);
        NamedIndex destination = new NamedIndex(destinationName, destinationIndex);
        if (!verticies.containsKey(source))
            throw new RuntimeException("No such source vertex: " + sourceIndex);
        else if (!verticies.containsKey(destination))
            throw new RuntimeException("No such destination vertex: " + destinationIndex);
        computePaths(vertex(sourceName, sourceIndex));

        Vertex dest = vertex(destinationName, destinationIndex);
        List<Vertex> path = new ArrayList<>();
        for (Vertex vertex = dest; vertex != null; vertex = vertex.previous)
            path.add(vertex);
        Collections.reverse(path);
        return path;
    }
}
