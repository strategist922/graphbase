package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core.IdGenerationStrategyT
import com.eaio.uuid.UUID
import org.apache.hadoop.hbase.util.Bytes

case class BinaryRandomIdGenerationStrategy() extends IdGenerationStrategyT[Array[Byte]] {

  def generateVertexId: Array[Byte] = {
    val rid = new UUID()
    Bytes.add(Bytes.toBytes(rid.getTime), Bytes.toBytes(rid.getClockSeqAndNode))
  }

  def generateEdgeLocalId: Array[Byte] = Bytes.toBytes(new UUID().getTime)

  def generateEdgeId(vertexId: Array[Byte], edgeLocalId: Array[Byte]): Array[Byte] = Bytes.add(vertexId, edgeLocalId)

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: Array[Byte]): Array[Byte] = Bytes.add(Bytes.toBytes(propertyKey), edgeLocalId)

  def getEdgeIdStruct(edgeId: Array[Byte]): (Array[Byte], Array[Byte]) = (Bytes.head(edgeId, 16), Bytes.tail(edgeId, 8))

  def getEdgePropertyIdStruct(edgePropertyId: Array[Byte]): (String, Array[Byte]) = (Bytes.toString(Bytes.head(edgePropertyId, edgePropertyId.length - 8)), Bytes.tail(edgePropertyId, 8))
}