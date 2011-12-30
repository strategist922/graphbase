package it.davidgreco.graphbase.core

trait IndexT[T, E <: ElementT[T]] {

  def name: String

  def elementClass: Class[_]

  def indexType: IndexType.Value

  def count(key: String, value: AnyRef): Long

  def get(key: String, value: AnyRef): Iterable[E]

  def put(key: String, value: AnyRef, element: E)

  def remove(key: String, value: AnyRef, element: E)

}

object IndexT {

  val EDGES: String = "edges"

  val VERTICES: String = "vertices"

}