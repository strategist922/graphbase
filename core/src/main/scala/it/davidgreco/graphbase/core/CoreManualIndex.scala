package it.davidgreco.graphbase.core

case class CoreManualIndex[T, E <: ElementT[T] : Manifest](repository: RepositoryT[T]) extends IndexT[T, E]{
  def name: String = null

  def elementClass = manifest[E].getClass

  def indexType = IndexType.manual

  def count(key: String, value: AnyRef): Long = 0

  def get(key: String, value: AnyRef): Iterable[E] = null

  def put(key: String, value: AnyRef, element: E) = null

  def remove(key: String, value: AnyRef, element: E) = null
}