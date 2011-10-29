package com.tinkerpop.blueprints.pgm.impls.graphbase

import java.util.Set
import com.tinkerpop.blueprints.pgm.{Vertex, Edge}
import it.davidgreco.graphbase.core.EdgeT
import collection.JavaConverters._

case class GraphbaseEdge[T](edge: EdgeT[T]) extends Edge {
  def getOutVertex: Vertex = edge.outVertex

  def getInVertex: Vertex = edge.inVertex

  def getLabel: String = edge.label

  def getProperty(key: String): AnyRef = edge.getProperty(key).getOrElse(null)

  def getPropertyKeys: Set[String] = edge.getPropertyKeys.asJava

  def setProperty(key: String, value: AnyRef): Unit = edge.setProperty(key, value)

  def removeProperty(key: String): AnyRef = edge.removeProperty(key).getOrElse(null)

  def getId: AnyRef = edge.id.asInstanceOf[AnyRef]

}