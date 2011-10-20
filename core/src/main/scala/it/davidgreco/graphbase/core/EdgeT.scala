package it.davidgreco.graphbase.core

private[core] trait EdgeT[T <: Comparable[T]] extends ElementT[T] {

  val outVertex: VertexT[T]

  val inVertex: VertexT[T]

  def label: String
}