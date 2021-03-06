package it.davidgreco.graphbase.core

trait GraphT[T] extends WithRepositoryT[T] {

  def addVertex() = repository.createVertex

  def getVertex(id: T): Option[VertexT[T]] = {
    if (id == null) throw new IllegalArgumentException
    repository.getVertex(id)
  }

  def removeVertex(vertex: VertexT[T]) {
    repository.removeVertex(vertex)
  }

  def getVertices: Iterable[VertexT[T]] = repository.getVertices()

  def addEdge(outVertex: VertexT[T], inVertex: VertexT[T], label: String): EdgeT[T] = repository.createEdge(outVertex, inVertex, label)

  def getEdge(id: T): Option[EdgeT[T]] = {
    if (id == null) throw new IllegalArgumentException
    repository.getEdge(id)
  }

  def removeEdge(edge: EdgeT[T]) {
    repository.removeEdge(edge)
  }

  def getEdges: Iterable[EdgeT[T]] = repository.getEdges()

  def clear() {
    repository.clear()
  }

  def shutdown() {
    repository.shutdown()
  }
}