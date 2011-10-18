package it.davidgreco.graphbase.core

trait WithRepositoryT[T <: Comparable[T]] {

  val repository: RepositoryT[T]

}