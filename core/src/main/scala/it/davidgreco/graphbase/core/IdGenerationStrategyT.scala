package it.davidgreco.graphbase.core

trait IdGenerationStrategyT {

  type IdType <: Comparable[IdType]

  def generateVertexId: IdType

  def generateEdgeLocalId: IdType

  def generateEdgeId(vertexId: IdType, edgeLocalId: IdType): IdType

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: IdType): IdType

  def getEdgeIdStruct(edgeId: IdType): (IdType, IdType)

  def getEdgePropertyIdStruct(edgePropertyId: IdType): (String, IdType)
}