package com.tinkerpop.blueprints.pgm.impls.graphbase

import java.lang.Iterable
import com.tinkerpop.blueprints.pgm.{Vertex, Edge, Graph}
import it.davidgreco.graphbase.core.{GraphT, CoreGraph, RepositoryT}

case class GraphbaseGraph(repository: RepositoryT) extends Graph {

  val coreGraph: GraphT = new CoreGraph(repository)

  def addVertex(id: AnyRef): Vertex = coreGraph.addVertex

  def getVertex(id: AnyRef): Vertex = coreGraph.getVertex(id) match {
    case Some(x) => x
    case None => null
  }

  def removeVertex(vertex: Vertex): Unit = coreGraph.removeVertex(vertex)

  def getVertices: Iterable[Vertex] = throw new UnsupportedOperationException();

  def addEdge(p1: AnyRef, outVertex: Vertex, inVertex: Vertex, label: String): Edge = coreGraph.addEdge(outVertex, inVertex, label)

  def getEdge(id: AnyRef): Edge = coreGraph.getEdge(id) match {
    case Some(x) => x
    case None => null
  }

  def removeEdge(edge: Edge): Unit = coreGraph.removeEdge(edge)

  def getEdges: Iterable[Edge] = throw new UnsupportedOperationException();

  def clear(): Unit = coreGraph.clear()

  def shutdown(): Unit = coreGraph.shutdown()
}