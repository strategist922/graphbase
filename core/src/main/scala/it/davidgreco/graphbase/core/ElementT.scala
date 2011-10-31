package it.davidgreco.graphbase.core

private[core] trait ElementT[T] extends WithRepositoryT[T] {

  self =>

  val id: T

  def getProperty(key: String): Option[AnyRef] = repository.getProperty(self, key)

  def getPropertyKeys: Set[String] = repository.getPropertyKeys(self)

  def setProperty(key: String, value: Any): Unit = repository.setProperty(self, key, value.asInstanceOf[AnyRef])

  def removeProperty(key: String): Option[AnyRef] = repository.removeProperty(self, key)

}