package it.davidgreco.graphbase.core

import sun.rmi.rmic.iiop.IDLGenerator

trait RepositoryT[T <: Comparable[T]] {

  val idGenerationStrategy: IdGenerationStrategyT[T]

  type IdType = IdGenerationStrategyT[T]#IdType

  //GraphT
  def shutdown(): Unit

  def clear(): Unit

  def createVertex: VertexT[T]

  def createEdge(out: VertexT[T], in: VertexT[T], label: String): EdgeT[T]

  def getVertex(id: T): Option[VertexT[T]]

  def getEdge(id: T): Option[EdgeT[T]]

  def removeEdge(edge: EdgeT[T]): Unit

  def removeVertex(vertex: VertexT[T]): Unit

  //ElementT
  def getProperty(element: ElementT[T], key: String): Option[AnyRef]

  def getPropertyKeys(element: ElementT[T]): Set[String]

  def removeProperty(element: ElementT[T], key: String): Option[AnyRef]

  def setProperty(element: ElementT[T], key: String, obj: AnyRef): Unit

  //VertexT
  def getInEdges(vertex: VertexT[T], labels: Seq[String]): Iterable[EdgeT[T]]

  def getOutEdges(vertex: VertexT[T], labels: Seq[String]): Iterable[EdgeT[T]]


}