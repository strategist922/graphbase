package it.davidgreco.graphbase.core

case class CoreGraph[T <: Comparable[T]](repository: RepositoryT[T]) extends GraphT[T]