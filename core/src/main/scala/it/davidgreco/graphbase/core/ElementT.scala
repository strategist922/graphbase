package it.davidgreco.graphbase.core

trait ElementT extends WithRepositoryT {

  self =>

  val id: AnyRef

  def getProperty(key: String): Option[AnyRef] = repository.getProperty(self, key)

  def getPropertyKeys: Set[String] = repository.getPropertyKeys(self)

  def setProperty(key: String, value: AnyRef): Unit = repository.setProperty(self, key, value)

  def removeProperty(key: String): Option[AnyRef] = repository.removeProperty(self, key)

}