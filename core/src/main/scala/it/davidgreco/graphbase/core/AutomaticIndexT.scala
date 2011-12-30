package it.davidgreco.graphbase.core

trait AutomaticIndexT[T, E <: ElementT[T]] extends IndexT[T, E] {

  def getAutoIndexKeys: Set[String]

}

object AutomaticIndexT {
  val LABEL: String = "label"
}
