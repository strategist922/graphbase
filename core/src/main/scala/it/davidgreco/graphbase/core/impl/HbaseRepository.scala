package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core._
import org.apache.hadoop.hbase.client.HBaseAdmin

case class HBaseRepository(admin: HBaseAdmin, name: String, idGenerationStrategy: IdGenerationStrategyT[Array[Byte]]) extends RepositoryT[Array[Byte]] {
  def shutdown() {}

  def clear() {}

  def createVertex = null

  def createEdge(out: VertexT[Array[Byte]], in: VertexT[Array[Byte]], label: String) = null

  def getVertex(id: Array[Byte]) = null

  def getEdge(id: Array[Byte]) = null

  def removeEdge(edge: EdgeT[Array[Byte]]) {}

  def removeVertex(vertex: VertexT[Array[Byte]]) {}

  def getVertices() = null

  def getEdges() = null

  def getProperty(element: ElementT[Array[Byte]], key: String) = null

  def getPropertyKeys(element: ElementT[Array[Byte]]) = null

  def removeProperty(element: ElementT[Array[Byte]], key: String) = null

  def setProperty(element: ElementT[Array[Byte]], key: String, obj: AnyRef) {}

  def getInEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]) = null

  def getOutEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]) = null
}