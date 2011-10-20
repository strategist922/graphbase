package it.davidgreco.graphbase.core.impl

object Main {

  def main(args: Array[String]) {

    val repository = MemoryBasedRepository("TABLE1", RandomIdGenerationStrategy())

    val graph = MemoryBasedGraph(repository)

    val v1 = graph.addVertex
    val v2 = graph.addVertex
    val e1 = graph.addEdge(v1, v2, "LABEL")

    val v1a = graph.getVertex(v1.id)
    val e1a = graph.getEdge(e1.id)

    println(v1 == v1a.get)
    println(e1 == e1a.get)

    println(v2.getInEdges(Seq("LABEL")) == v2.getInEdges(Seq()))
    println(v1.getOutEdges(Seq("LABEL")) == v1.getOutEdges(Seq()))

    graph.removeEdge(e1a.get)

    val e1b = graph.getEdge(e1.id)

    println(e1b == None)

    graph.removeVertex(v1)
    graph.removeVertex(v2)

  }

}