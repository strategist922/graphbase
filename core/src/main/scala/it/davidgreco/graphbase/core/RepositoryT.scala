package it.davidgreco.graphbase.core

trait RepositoryT[T <: Comparable[T]] extends WithIdGenerationStrategyT[T] {

  type IdType = T

  //GraphT
  def shutdown(): Unit

  def clear(): Unit

  def createVertex: VertexT[T]

  def createEdge(out: VertexT[T], in: VertexT[T], label: String): EdgeT[T]

  def getVertex(id: T): Option[VertexT[T]]

  def getEdge(id: T): Option[EdgeT[T]]

  def removeEdge(edge: EdgeT[T]): Unit

  def removeVertex(vertex: VertexT[T]): Unit

  def getVertices(): Iterable[VertexT[T]]

  def getEdges(): Iterable[EdgeT[T]]

  //ElementT
  def getProperty(element: ElementT[T], key: String): Option[AnyRef]

  def getPropertyKeys(element: ElementT[T]): Set[String]

  def removeProperty(element: ElementT[T], key: String): Option[AnyRef]

  def setProperty(element: ElementT[T], key: String, obj: AnyRef): Unit

  //VertexT
  def getInEdges(vertex: VertexT[T], labels: Seq[String]): Iterable[EdgeT[T]]

  def getOutEdges(vertex: VertexT[T], labels: Seq[String]): Iterable[EdgeT[T]]


}