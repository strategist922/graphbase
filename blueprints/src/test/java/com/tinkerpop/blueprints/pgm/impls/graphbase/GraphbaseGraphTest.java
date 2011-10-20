package com.tinkerpop.blueprints.pgm.impls.graphbase;


import com.tinkerpop.blueprints.pgm.EdgeTestSuite;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.VertexTestSuite;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import it.davidgreco.graphbase.core.impl.MemoryBasedRepository;
import it.davidgreco.graphbase.core.impl.RandomIdGenerationStrategy;

import java.lang.reflect.Method;

public class GraphbaseGraphTest extends GraphTest {

    public GraphbaseGraphTest() {
        this.allowsDuplicateEdges = true;
        this.allowsSelfLoops = true;
        this.ignoresSuppliedIds = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = true;
        this.supportsEdgeIteration = true;
        this.supportsVertexIndex = false;
        this.supportsEdgeIndex = false;
        this.supportsTransactions = false;
    }

    @Override
    public Graph getGraphInstance() {
        return new GraphbaseGraph(new MemoryBasedRepository("GRAPH", new RandomIdGenerationStrategy()));
    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    @Override
    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testGraphbaseGraph");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }
    }

}
