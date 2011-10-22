package it.davidgreco.graphbase.core

case class CoreEdge[T](id: T, outVertex: VertexT[T], inVertex: VertexT[T], label: String, repository: RepositoryT[T]) extends EdgeT[T]

private[core] object CoreEdge