package it.davidgreco.graphbase.core

private[core] trait WithIdGenerationStrategyT[T] {

  val idGenerationStrategy: IdGenerationStrategyT[T]

}