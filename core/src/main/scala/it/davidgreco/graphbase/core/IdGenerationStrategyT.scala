package it.davidgreco.graphbase.core

private[core] trait IdGenerationStrategyT[T] {

  def generateVertexId: T

  def generateEdgeLocalId: T

  def generateEdgeId(vertexId: T, edgeLocalId: T): T

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: T): T

  def getEdgeIdStruct(edgeId: T): (T, T)

  def getEdgePropertyIdStruct(edgePropertyId: T): (String, T)
}