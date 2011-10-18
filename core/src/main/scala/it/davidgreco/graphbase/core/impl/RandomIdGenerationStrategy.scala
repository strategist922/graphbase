package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core.IdGenerationStrategyT
import com.eaio.uuid.UUID

class RandomIdGenerationStrategy extends IdGenerationStrategyT[String] {

  def generateVertexId: RandomIdGenerationStrategy#IdType = {
    val rid: UUID = new UUID
    rid.toString
  }

  def generateEdgeLocalId: RandomIdGenerationStrategy#IdType = {
    val rid: UUID = new UUID
    rid.getTime.toHexString
  }

  def generateEdgeId(vertexId: RandomIdGenerationStrategy#IdType, edgeLocalId: RandomIdGenerationStrategy#IdType): RandomIdGenerationStrategy#IdType = vertexId.asInstanceOf[String]+'#'+edgeLocalId.asInstanceOf[String]

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: RandomIdGenerationStrategy#IdType): RandomIdGenerationStrategy#IdType = propertyKey+'#'+edgeLocalId.asInstanceOf[String]

  def getEdgeIdStruct(edgeId: RandomIdGenerationStrategy#IdType): (RandomIdGenerationStrategy#IdType, RandomIdGenerationStrategy#IdType) = {
    val fields = edgeId.asInstanceOf[String].split('#')
    (fields.apply(0), fields.apply(1))
  }

  def getEdgePropertyIdStruct(edgePropertyId: RandomIdGenerationStrategy#IdType): (String, RandomIdGenerationStrategy#IdType) = {
    val fields = edgePropertyId.asInstanceOf[String].split('#')
    (fields.apply(0).asInstanceOf[String], fields.apply(1))
  }

}