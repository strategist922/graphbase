package it.davidgreco.graphbase.core

trait RepositoryT {

  //GraphT
  def shutdown(): Unit

  def clear(): Unit

  val idGenerationStrategy: IdGenerationStrategyT

  def createVertex: VertexT

  def createEdge(out: VertexT, in: VertexT, label: String): EdgeT

  def getVertex(id: AnyRef): Option[VertexT]

  def getEdge(id: AnyRef): Option[EdgeT]

  def removeEdge(edge: EdgeT): Unit

  def removeVertex(vertex: VertexT): Unit

  //ElementT
  def getProperty(element: ElementT, key: String): Option[AnyRef]

  def getPropertyKeys(element: ElementT): Set[String]

  def removeProperty(element: ElementT, key: String): Option[AnyRef]

  def setProperty(element: ElementT, key: String, obj: AnyRef): Unit

  //VertexT
  def getInEdges(vertex: VertexT, labels: Seq[String]): Iterable[EdgeT]

  def getOutEdges(vertex: VertexT, labels: Seq[String]): Iterable[EdgeT]


}