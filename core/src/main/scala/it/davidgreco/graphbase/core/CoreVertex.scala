package it.davidgreco.graphbase.core

case class CoreVertex[T](id: T, repository: RepositoryT[T]) extends VertexT[T]

private[core] object CoreVertex
