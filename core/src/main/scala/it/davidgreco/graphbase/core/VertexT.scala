package it.davidgreco.graphbase.core

trait VertexT[T] extends ElementT[T] {

  def getOutEdges(labels: Seq[String]): Iterable[EdgeT[T]] = {
    repository.getOutEdges(this, labels)
  }

  def getInEdges(labels: Seq[String]): Iterable[EdgeT[T]] = {
    repository.getInEdges(this, labels)
  }

}