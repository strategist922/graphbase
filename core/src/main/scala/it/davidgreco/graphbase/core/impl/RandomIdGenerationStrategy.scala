package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core.IdGenerationStrategyT
import com.eaio.uuid.UUID

case class RandomIdGenerationStrategy() extends IdGenerationStrategyT[String] {

  def generateVertexId: String = {
    val rid: UUID = new UUID
    rid.toString
  }

  def generateEdgeLocalId: String = {
    val rid: UUID = new UUID
    rid.getTime.toHexString
  }

  def generateEdgeId(vertexId: String, edgeLocalId: String): String = vertexId.asInstanceOf[String]+'#'+edgeLocalId.asInstanceOf[String]

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: String): String = propertyKey+'#'+edgeLocalId.asInstanceOf[String]

  def getEdgeIdStruct(edgeId: String): (String, String) = {
    val fields = edgeId.asInstanceOf[String].split('#')
    (fields.apply(0), fields.apply(1))
  }

  def getEdgePropertyIdStruct(edgePropertyId: String): (String, String) = {
    val fields = edgePropertyId.asInstanceOf[String].split('#')
    (fields.apply(0).asInstanceOf[String], fields.apply(1))
  }

}