package it.davidgreco.graphbase.core

trait EdgeT[T] extends ElementT[T] {

  val outVertex: VertexT[T]

  val inVertex: VertexT[T]

  def label: String
}