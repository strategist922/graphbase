package com.tinkerpop.blueprints.pgm.impls.graphbase

import collection.JavaConverters._
import java.lang.Iterable
import com.tinkerpop.blueprints.pgm.{Vertex, Edge, Graph}
import it.davidgreco.graphbase.core.GraphT
import it.davidgreco.graphbase.core.impl.{HBaseGraph, HBaseRepository}

case class GraphbaseGraph(quorum: String, port: String, name: String) extends Graph {

  val coreGraph: GraphT[Array[Byte]] = HBaseGraph(HBaseRepository(quorum, port, name))

  def addVertex(id: AnyRef): Vertex = coreGraph.addVertex()

  def getVertex(id: AnyRef): Vertex = coreGraph.getVertex(id.asInstanceOf[Array[Byte]]) match {
    case Some(x) => x
    case None => null
  }

  def removeVertex(vertex: Vertex) {
    coreGraph.removeVertex(vertex)
  }

  def getVertices: Iterable[Vertex] = {
    (for {
      vertext <- coreGraph.getVertices
      vertex: Vertex = vertext
    } yield vertex).asJava
  }

  def addEdge(p1: AnyRef, outVertex: Vertex, inVertex: Vertex, label: String): Edge = coreGraph.addEdge(outVertex, inVertex, label)

  def getEdge(id: AnyRef): Edge = coreGraph.getEdge(id.asInstanceOf[Array[Byte]]) match {
    case Some(x) => x
    case None => null
  }

  def removeEdge(edge: Edge) {
    coreGraph.removeEdge(edge)
  }

  def getEdges: Iterable[Edge] = {
    (for {
      edget <- coreGraph.getEdges
      edge: Edge = edget
    } yield edge).asJava
  }

  def clear() {
    coreGraph.clear()
  }

  def shutdown() {
    coreGraph.shutdown()
  }
}