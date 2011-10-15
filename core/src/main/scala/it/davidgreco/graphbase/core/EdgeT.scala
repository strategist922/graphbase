package it.davidgreco.graphbase.core

trait EdgeT extends ElementT {

  val outVertex: VertexT

  val inVertex: VertexT

  def label: String
}