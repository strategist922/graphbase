package com.tinkerpop.blueprints.pgm.impls.graphbase;

import com.tinkerpop.blueprints.pgm.EdgeTestSuite;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.VertexTestSuite;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import it.davidgreco.graphbase.core.impl.HBaseRepository;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.reflect.Method;

public class GraphbaseGraphTest extends GraphTest {

    private HBaseTestingUtility testUtil = new HBaseTestingUtility();

    @BeforeClass
    public void setUp() throws Exception {
        testUtil.startMiniCluster();
    }

    @AfterClass
    public void tearDown() throws IOException {
        testUtil.shutdownMiniCluster();
    }

    public GraphbaseGraphTest() {
        this.allowsDuplicateEdges = true;
        this.allowsSelfLoops = false;
        this.ignoresSuppliedIds = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = false;
        this.supportsEdgeIteration = false;
        this.supportsVertexIndex = false;
        this.supportsEdgeIndex = false;
        this.supportsTransactions = false;
    }

    @Override
    public Graph getGraphInstance() {
        Graph graph = new GraphbaseGraph("localhost", "21818", "Graph");
        graph.clear();
        return graph;
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
                    if (method.getName() != "testNoConcurrentModificationException") {
                        System.out.println("Testing " + method.getName() + "...");
                        method.invoke(testSuite);
                    }
                }
            }
        }
    }

}
