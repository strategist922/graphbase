package com.tinkerpop.blueprints.pgm.impls.graphbase;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Vertex;
import it.davidgreco.graphbase.core.impl.MemoryBasedRepository;
import it.davidgreco.graphbase.core.impl.RandomIdGenerationStrategy;

import java.util.Set;

public class Main {

    public static void main(String[] args) {

        RandomIdGenerationStrategy idGenerationStrategy = new RandomIdGenerationStrategy();
        MemoryBasedRepository repository = new MemoryBasedRepository("GRAPH", idGenerationStrategy);
        Graph g = (Graph) new GraphbaseGraph(repository);
        Vertex v1 = g.addVertex(null);
        Vertex v2 = g.addVertex(null);
        Edge e1 = g.addEdge(null, v1, v2, "LABEL");

        e1.setProperty("NAME", "DAVID");
        e1.setProperty("COGNOME", "DAVID");

        Set<String> ekeys = e1.getPropertyKeys();

        Vertex v1a = g.getVertex(v1.getId());

        System.out.println(v1.getId() == v1a.getId());

    }

}
