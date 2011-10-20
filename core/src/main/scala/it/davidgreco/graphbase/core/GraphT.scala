package it.davidgreco.graphbase.core

private[core] trait GraphT[T <: Comparable[T]] {

  val repository: RepositoryT[T]

  def addVertex: VertexT[T] = repository.createVertex

  def getVertex(id: T): Option[VertexT[T]] = repository.getVertex(id)

  def removeVertex(vertex: VertexT[T]) = repository.removeVertex(vertex)

  def getVertices: Iterable[VertexT[T]] = repository.getVertices

  def addEdge(outVertex: VertexT[T], inVertex: VertexT[T], label: String): EdgeT[T] = repository.createEdge(outVertex, inVertex, label)

  def getEdge(id: T): Option[EdgeT[T]] = repository.getEdge(id)

  def removeEdge(edge: EdgeT[T]) = repository.removeEdge(edge)

  def getEdges: Iterable[EdgeT[T]] = repository.getEdges()

  def clear() = repository.clear()

  def shutdown() = repository.shutdown()
}