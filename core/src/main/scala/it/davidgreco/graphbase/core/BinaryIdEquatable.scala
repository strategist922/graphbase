package it.davidgreco.graphbase.core

import java.util.Arrays

trait BinaryIdEquatable[T <: {val id: Array[Byte]}] {

  self: T =>

  override def equals(other: Any): Boolean = Arrays.equals(self.id, other.asInstanceOf[T].id)

  override def hashCode(): Int = Arrays.hashCode(self.id)
}
