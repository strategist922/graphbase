package it.davidgreco.graphbase.core

trait IdGenerationStrategyT[T <: Comparable[T]] {

  type IdType = T

  def generateVertexId: T

  def generateEdgeLocalId: T

  def generateEdgeId(vertexId: T, edgeLocalId: T): T

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: T): T

  def getEdgeIdStruct(edgeId: T): (T, T)

  def getEdgePropertyIdStruct(edgePropertyId: T): (String, T)
}