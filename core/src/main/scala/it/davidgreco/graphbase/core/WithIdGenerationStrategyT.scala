package it.davidgreco.graphbase.core

private[core] trait WithIdGenerationStrategyT[T <: Comparable[T]] {

  val idGenerationStrategy: IdGenerationStrategyT[T]

}