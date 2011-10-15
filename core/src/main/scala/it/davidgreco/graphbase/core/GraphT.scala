package it.davidgreco.graphbase.core

trait GraphT {

  val repository: RepositoryT

  def addVertex: VertexT = repository.createVertex

  def getVertex(id: AnyRef): Option[VertexT] = repository.getVertex(id)

  def removeVertex(vertex: VertexT) = repository.removeVertex(vertex)

  def getVertices: Iterable[VertexT] = null

  def addEdge(outVertex: VertexT, inVertex: VertexT, label: String): EdgeT = repository.createEdge(outVertex, inVertex, label)

  def getEdge(id: AnyRef): Option[EdgeT] = repository.getEdge(id)

  def removeEdge(edge: EdgeT) = repository.removeEdge(edge)

  def getEdges: Iterable[EdgeT] = null

  def clear() = repository.clear()

  def shutdown() = repository.shutdown()
}