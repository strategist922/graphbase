package it.davidgreco.graphbase.core

private[core] trait WithRepositoryT[T] {

  def repository: RepositoryT[T]

}