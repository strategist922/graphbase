package it.davidgreco.graphbase.core

trait IdGenerationStrategyT {

  def generateVertexId: AnyRef

  def generateEdgeLocalId: AnyRef

  def generateEdgeId(vertexId: AnyRef, edgeLocalId: AnyRef): AnyRef

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: AnyRef): AnyRef

  def getEdgeIdStruct(edgeId: AnyRef): (AnyRef, AnyRef)

  def getEdgePropertyIdStruct(edgePropertyId: AnyRef): (String, AnyRef)
}