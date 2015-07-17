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


/**
 * Adapted from http://www.algolist.com/code/java/Dijkstra's_algorithm.
 * <p/>
 * See also:  http://en.wikipedia.org/wiki/Dijkstra's_algorithm
 */
public class Dijkstra {
    public class Vertex implements Comparable<Vertex>, Iterable<Edge> {
        private final String name;
        private final List<Edge> adjacencies = new ArrayList<>();
        private double minDistance = Double.POSITIVE_INFINITY;
        private Vertex previous;

        private Vertex(String argName) {
            name = argName;
        }

        public String toString() {
            return name;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return name.equals(((Vertex) obj).name);
        }

        public int compareTo(Vertex other) {
            return Double.compare(minDistance, other.minDistance);
        }

        @Override
        public Iterator<Edge> iterator() {
            return new ArrayList<Edge>(adjacencies).iterator();
        }

        public Vertex add(String name, double weight) {
            Edge e;
            add(e = edge(name, weight));
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

        public List<Edge> adjacencies() {
            return adjacencies;
        }

        private void reset() {
            minDistance = Double.POSITIVE_INFINITY;
            previous = null;
            for (Edge e : adjacencies) {
                e.reset();
            }
        }
    }

    public class Edge {
        public final Vertex target;
        public final double weight;

        private Edge(Vertex argTarget, double argWeight) {
            target = argTarget;
            weight = argWeight;
        }

        private void reset() {
            if (target.previous != null)
                target.reset();
        }
    }

    private final Map<String, Vertex> verticies = new HashMap<>();

    public Vertex vertex(String name) {
        Vertex v = verticies.get(name);
        if (v == null)
            verticies.put(name, v = new Vertex(name));
        return v;
    }

    public Edge edge(String name, double weight) {
        return new Edge(vertex(name), weight);
    }

    private void computePaths(Vertex source) {
        for (Vertex v : verticies.values())
            v.reset();

        PriorityQueue<Vertex> vertexQueue = new PriorityQueue<Vertex>();
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

    public List<Vertex> getShortestPathTo(String source, String destination) {
        if (!verticies.containsKey(source))
            throw new RuntimeException("No such source vertex: " + source);
        else if (!verticies.containsKey(destination))
            throw new RuntimeException("No such destination vertex: " + destination);
        computePaths(vertex(source));

        Vertex dest = vertex(destination);
        List<Vertex> path = new ArrayList<Vertex>();
        for (Vertex vertex = dest; vertex != null; vertex = vertex.previous)
            path.add(vertex);
        Collections.reverse(path);
        return path;
    }
}
