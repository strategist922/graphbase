package com.tinkerpop.blueprints.pgm.impls.graphbase

import collection.JavaConverters._
import java.lang.Iterable
import com.tinkerpop.blueprints.pgm.{Vertex, Edge, Graph}
import it.davidgreco.graphbase.core.{GraphT, CoreGraph, RepositoryT}

case class GraphbaseGraph[T <: Comparable[T]](repository: RepositoryT[T]) extends Graph {

  val coreGraph: GraphT[T] = new CoreGraph[T](repository)

  def addVertex(id: AnyRef): Vertex = coreGraph.addVertex

  def getVertex(id: AnyRef): Vertex = coreGraph.getVertex(id.asInstanceOf[T]) match {
    case Some(x) => x
    case None => null
  }

  def removeVertex(vertex: Vertex): Unit = coreGraph.removeVertex(vertex)

  def getVertices: Iterable[Vertex] = {
    (for {
      vertext <- coreGraph.getVertices
      vertex: Vertex = vertext
    } yield vertex).asJava
  }

  def addEdge(p1: AnyRef, outVertex: Vertex, inVertex: Vertex, label: String): Edge = coreGraph.addEdge(outVertex, inVertex, label)

  def getEdge(id: AnyRef): Edge = coreGraph.getEdge(id.asInstanceOf[T]) match {
    case Some(x) => x
    case None => null
  }

  def removeEdge(edge: Edge): Unit = coreGraph.removeEdge(edge)

  def getEdges: Iterable[Edge] = {
    (for {
      edget <- coreGraph.getEdges
      edge: Edge = edget
    } yield edge).asJava
  }

  def clear(): Unit = coreGraph.clear()

  def shutdown(): Unit = coreGraph.shutdown()
}