package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core.IdGenerationStrategyT
import com.eaio.uuid.UUID

class RandomIdGenerationStrategy extends IdGenerationStrategyT {

  def generateVertexId: AnyRef = {
    val rid: UUID = new UUID
    rid.toString
  }

  def generateEdgeLocalId: AnyRef = {
    val rid: UUID = new UUID
    rid.getTime.toHexString
  }

  def generateEdgeId(vertexId: AnyRef, edgeLocalId: AnyRef): AnyRef = vertexId.asInstanceOf[String]+'#'+edgeLocalId.asInstanceOf[String]

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: AnyRef): AnyRef = propertyKey+'#'+edgeLocalId.asInstanceOf[String]

  def getEdgeIdStruct(edgeId: AnyRef): (AnyRef, AnyRef) = {
    val fields = edgeId.asInstanceOf[String].split('#')
    (fields.apply(0), fields.apply(1))
  }

  def getEdgePropertyIdStruct(edgePropertyId: AnyRef): (String, AnyRef) = {
    val fields = edgePropertyId.asInstanceOf[String].split('#')
    (fields.apply(0).asInstanceOf[String], fields.apply(1))
  }
}