package it.davidgreco.graphbase.core

private[core] trait WithRepositoryT[T <: Comparable[T]] {

  val repository: RepositoryT[T]

}