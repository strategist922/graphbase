package it.davidgreco.graphbase.core

private[core] trait WithIdGenerationStrategyT[T] {

  def idGenerationStrategy: IdGenerationStrategyT[T]

}