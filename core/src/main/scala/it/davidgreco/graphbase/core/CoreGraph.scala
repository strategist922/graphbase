package it.davidgreco.graphbase.core

case class CoreGraph[T](repository: RepositoryT[T]) extends GraphT[T]

private[core] object CoreGraph