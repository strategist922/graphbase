package it.davidgreco.graphbase.core

private[core] trait ElementT[T <: Comparable[T]] extends WithRepositoryT[T] {

  self =>

  val id: T

  def getProperty(key: String): Option[AnyRef] = repository.getProperty(self, key)

  def getPropertyKeys: Set[String] = repository.getPropertyKeys(self)

  def setProperty(key: String, value: AnyRef): Unit = repository.setProperty(self, key, value)

  def removeProperty(key: String): Option[AnyRef] = repository.removeProperty(self, key)

}