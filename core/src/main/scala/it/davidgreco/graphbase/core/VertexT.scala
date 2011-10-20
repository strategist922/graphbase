package it.davidgreco.graphbase.core

private[core] trait VertexT[T <: Comparable[T]] extends ElementT[T] {

  def getOutEdges(labels: Seq[String]): Iterable[EdgeT[T]] = {
    repository.getOutEdges(this, labels)
  }

  def getInEdges(labels: Seq[String]): Iterable[EdgeT[T]] = {
    repository.getInEdges(this, labels)
  }

}