package it.davidgreco.graphbase.core

private[core] trait WithRepositoryT[T] {

  val repository: RepositoryT[T]

}