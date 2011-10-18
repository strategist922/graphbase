package com.tinkerpop.blueprints.pgm.impls.graphbase

import java.util.Set
import java.lang.Iterable
import com.tinkerpop.blueprints.pgm.{Edge, Vertex}
import it.davidgreco.graphbase.core.VertexT
import collection.JavaConverters._

case class GraphbaseVertex[T <: Comparable[T]](vertex: VertexT[T]) extends Vertex {
  def getOutEdges(labels: java.lang.String*): Iterable[Edge] = {
    val slabels = for {
      l <- labels
    } yield l.asInstanceOf[String]
    (for {
      e <- vertex.getOutEdges(slabels)
    } yield edgeToGraphbaseEdge(e)).asJava
  }

  def getInEdges(labels: java.lang.String*): Iterable[Edge] = {
    val slabels = for {
      l <- labels
    } yield l.asInstanceOf[String]
    (for {
      e <- vertex.getInEdges(slabels)
    } yield edgeToGraphbaseEdge(e)).asJava
  }

  def getProperty(key: String): AnyRef = vertex.getProperty(key).getOrElse(null)

  def getPropertyKeys: Set[java.lang.String] = vertex.getPropertyKeys.asJava

  def setProperty(key: String, value: AnyRef): Unit = vertex.setProperty(key, value)

  def removeProperty(key: String): AnyRef = vertex.removeProperty(key).getOrElse(null)

  def getId: AnyRef = vertex.id

}