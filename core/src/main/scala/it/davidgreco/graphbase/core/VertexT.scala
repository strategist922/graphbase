package it.davidgreco.graphbase.core

trait VertexT extends ElementT {

  def getOutEdges(labels: Seq[String]): Iterable[EdgeT] = {
    repository.getOutEdges(this, labels)
  }

  def getInEdges(labels: Seq[String]): Iterable[EdgeT] = {
    repository.getInEdges(this, labels)
  }

}