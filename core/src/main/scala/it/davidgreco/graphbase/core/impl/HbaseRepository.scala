package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core._
import java.io.IOException
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, MasterNotRunningException, ZooKeeperConnectionException}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.hbase.util.Bytes

case class HBaseRepository(admin: HBaseAdmin, name: String, idGenerationStrategy: IdGenerationStrategyT[Array[Byte]]) extends RepositoryT[Array[Byte]] {

  private val tableName = name + '$' + "GRAPH"
  private val vertexPropertiesColumnFamily = tableName + '$' + "VERTEXPROPERTIES"
  private val edgePropertiesColumnFamily = tableName + '$' + "EDGEPROPERTIES"
  private val inEdgesColumnFamily = tableName + '$' + "INEDGES"
  private val outEdgesColumnFamily = tableName + '$' + "OUTEDGES"
  private var table: HTable = _


  private def createTables = {
    try {
      if (!admin.tableExists(tableName)) {
        admin.createTable(new HTableDescriptor(tableName));
        admin.disableTable(tableName);
        admin.addColumn(tableName, new HColumnDescriptor(vertexPropertiesColumnFamily));
        admin.addColumn(tableName, new HColumnDescriptor(edgePropertiesColumnFamily));
        admin.addColumn(tableName, new HColumnDescriptor(inEdgesColumnFamily));
        admin.addColumn(tableName, new HColumnDescriptor(outEdgesColumnFamily));
        admin.enableTable(tableName);
      }
      table = new HTable(admin.getConfiguration(), tableName);
    } catch {
      case e: MasterNotRunningException => throw new RuntimeException(e)
      case e: ZooKeeperConnectionException => throw new RuntimeException(e)
      case e: IOException => throw new RuntimeException(e)
    }
  }

  private def dropTables = {
    try {
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    } catch {
      case e: MasterNotRunningException => throw new RuntimeException(e)
      case e: ZooKeeperConnectionException => throw new RuntimeException(e)
      case e: IOException => throw new RuntimeException(e)
    }
  }


  def shutdown(): Unit = {}

  def clear(): Unit = {
    dropTables
    createTables
  }

  def createVertex: VertexT[IdType] = {
    val id = idGenerationStrategy.generateVertexId

    val put = new Put(id);
    put.add(Bytes.toBytes(vertexPropertiesColumnFamily), null, null);
    table.put(put);

    CoreVertex(id, this)
  }

  def createEdge(out: VertexT[Array[Byte]], in: VertexT[Array[Byte]], label: String): EdgeT[IdType] = {
    val eli = idGenerationStrategy.generateEdgeLocalId
    val id = idGenerationStrategy.generateEdgeId(out.id, eli)

    /*
    var rowOut = table.get(out.id)
    var rowIn = table.get(in.id)
    if (!(rowIn.isDefined && rowOut.isDefined)) {
      throw new RuntimeException("One or both vertexes don't exist");
    }
    rowOut.get("OUTEDGES") += eli -> in.id
    rowOut.get("EDGEPROPERTIES") += idGenerationStrategy.generateEdgePropertyId("label", eli) -> label
    rowIn.get("INEDGES") += eli -> id */

    CoreEdge(id, out, in, label, this)
  }

  def getVertex(id: Array[Byte]) = null

  def getEdge(id: Array[Byte]) = null

  def removeEdge(edge: EdgeT[Array[Byte]]) {}

  def removeVertex(vertex: VertexT[Array[Byte]]) {}

  def getVertices() = null

  def getEdges() = null

  def getProperty(element: ElementT[Array[Byte]], key: String) = null

  def getPropertyKeys(element: ElementT[Array[Byte]]) = null

  def removeProperty(element: ElementT[Array[Byte]], key: String) = null

  def setProperty(element: ElementT[Array[Byte]], key: String, obj: AnyRef) {}

  def getInEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]) = null

  def getOutEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]) = null
}