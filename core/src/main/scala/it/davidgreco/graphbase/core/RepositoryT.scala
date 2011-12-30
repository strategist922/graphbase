package it.davidgreco.graphbase.core

trait RepositoryT[T] extends WithIdGenerationStrategyT[T] {

  type IdType = T

  //GraphT
  def shutdown()

  def clear()

  def createVertex: VertexT[T]

  def createEdge(out: VertexT[T], in: VertexT[T], label: String): EdgeT[T]

  def getVertex(id: T): Option[VertexT[T]]

  def getEdge(id: T): Option[EdgeT[T]]

  def removeEdge(edge: EdgeT[T])

  def removeVertex(vertex: VertexT[T])

  def getVertices(): Iterable[VertexT[T]]

  def getEdges(): Iterable[EdgeT[T]]

  //ElementT
  def getProperty(element: ElementT[T], key: String): Option[AnyRef]

  def getPropertyKeys(element: ElementT[T]): Set[String]

  def removeProperty(element: ElementT[T], key: String): Option[AnyRef]

  def setProperty(element: ElementT[T], key: String, obj: AnyRef)

  //VertexT
  def getInEdges(vertex: VertexT[T], labels: Seq[String]): Iterable[EdgeT[T]]

  def getOutEdges(vertex: VertexT[T], labels: Seq[String]): Iterable[EdgeT[T]]

  //Index


}