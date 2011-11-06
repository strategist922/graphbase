package it.davidgreco.graphbase.core

trait EdgeT[T] extends ElementT[T] {

  def outVertex: VertexT[T]

  def inVertex: VertexT[T]

  def label: String
}