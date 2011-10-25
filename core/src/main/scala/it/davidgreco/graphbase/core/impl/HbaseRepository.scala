package it.davidgreco.graphbase.core.impl

import collection.JavaConverters._
import java.io.IOException
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import util.Bytes
import it.davidgreco.graphbase.core._

case class HBaseRepository(quorum: String, port: String, name: String, idGenerationStrategy: IdGenerationStrategyT[Array[Byte]]) extends RepositoryT[Array[Byte]] {

  private val admin = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    try {
      new HBaseAdmin(conf);
    } catch {
      case e: MasterNotRunningException => throw new RuntimeException(e)
      case e: ZooKeeperConnectionException => throw new RuntimeException(e)
    }
  }

  private val tableName = name + '_' + "GRAPH"

  private val vertexPropertiesColumnFamilyName = tableName + '_' + "VERTEXPROPERTIES"
  private val edgePropertiesColumnFamilyName = tableName + '_' + "EDGEPROPERTIES"
  private val inEdgesColumnFamilyName = tableName + '_' + "INEDGES"
  private val outEdgesColumnFamilyName = tableName + '_' + "OUTEDGES"

  private val vertexPropertiesColumnFamily: Array[Byte] = Bytes.toBytes(vertexPropertiesColumnFamilyName)
  private val edgePropertiesColumnFamily: Array[Byte] = Bytes.toBytes(edgePropertiesColumnFamilyName)
  private val inEdgesColumnFamily: Array[Byte] = Bytes.toBytes(inEdgesColumnFamilyName)
  private val outEdgesColumnFamily: Array[Byte] = Bytes.toBytes(outEdgesColumnFamilyName)
  private var table: HTable = _

  createTables

  private def createTables = {
    try {
      if (!admin.tableExists(tableName)) {
        admin.createTable(new HTableDescriptor(tableName));
        admin.disableTable(tableName);
        admin.addColumn(tableName, new HColumnDescriptor(vertexPropertiesColumnFamilyName));
        admin.addColumn(tableName, new HColumnDescriptor(edgePropertiesColumnFamilyName));
        admin.addColumn(tableName, new HColumnDescriptor(inEdgesColumnFamilyName));
        admin.addColumn(tableName, new HColumnDescriptor(outEdgesColumnFamilyName));
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
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
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
    put.add(vertexPropertiesColumnFamily, null, null);
    table.put(put);

    new CoreVertex[IdType](id, this) with BinaryIdEquatable[CoreVertex[IdType]]
  }

  def createEdge(out: VertexT[Array[Byte]], in: VertexT[Array[Byte]], label: String): EdgeT[IdType] = {
    val eli = idGenerationStrategy.generateEdgeLocalId
    val id = idGenerationStrategy.generateEdgeId(out.id, eli)

    val outGet = new Get(out.id)
    val inGet = new Get(in.id)
    val rowOut = table.get(outGet)
    val rowIn = table.get(inGet)

    if (rowOut.isEmpty || rowIn.isEmpty) {
      throw new RuntimeException("One or both vertexes don't exist")
    }

    val outPut = new Put(out.id)
    outPut.add(outEdgesColumnFamily, eli, in.id)
    val blabel: Array[Byte] = label
    outPut.add(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId("label", eli), blabel)

    val inPut = new Put(in.id)
    inPut.add(inEdgesColumnFamily, eli, id)

    table.put(List(outPut, inPut).asJava)

    new CoreEdge[IdType](id, out, in, label, this) with BinaryIdEquatable[CoreEdge[IdType]]
  }

  def getVertex(id: Array[Byte]): Option[VertexT[IdType]] = {
    if (id == null)
      return None;
    val rowGet = new Get(id)
    val row = table.get(rowGet)
    if (row.isEmpty)
      None
    else {
      Some(new CoreVertex[IdType](id, this) with BinaryIdEquatable[CoreVertex[IdType]])
    }
  }

  def getEdge(id: Array[Byte]): Option[EdgeT[IdType]] = {
    if (id == null)
      return None;
    val struct = idGenerationStrategy.getEdgeIdStruct(id)
    val outGet = new Get(struct._1)
    val rowOut = table.get(outGet)
    if (rowOut.isEmpty)
      return None

    val inId: Array[Byte] = rowOut.getValue(outEdgesColumnFamily, struct._2)
    if (inId == null)
      return None

    val label: String = rowOut.getValue(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId("label", struct._2))

    val outVertex = new CoreVertex[IdType](struct._1, this) with BinaryIdEquatable[CoreVertex[IdType]]
    val inVertex = new CoreVertex[IdType](inId, this) with BinaryIdEquatable[CoreVertex[IdType]]
    Some(new CoreEdge[IdType](id, outVertex, inVertex, label, this) with BinaryIdEquatable[CoreEdge[IdType]])
  }

  def removeEdge(edge: EdgeT[Array[Byte]]): Unit = {
    val outGet = new Get(edge.outVertex.id)
    val inGet = new Get(edge.inVertex.id)
    val rowOut = table.get(outGet)
    val rowIn = table.get(inGet)

    if (rowOut.isEmpty || rowIn.isEmpty) {
      throw new RuntimeException("One or both vertexes don't exist");
    }

    val struct = idGenerationStrategy.getEdgeIdStruct(edge.id)

    val deleteOut = new Delete(rowOut.getRow())
    deleteOut.deleteColumns(outEdgesColumnFamily, struct._2);

    val deleteIn = new Delete(rowIn.getRow())
    deleteIn.deleteColumns(inEdgesColumnFamily, struct._2);

    edge.getPropertyKeys map (p => edge.removeProperty(p))

    table.delete(List(deleteOut, deleteIn).asJava)
  }

  def removeVertex(vertex: VertexT[Array[Byte]]): Unit = {
    /*
    for (edge <- this.getOutEdges(vertex, Seq[String]())) {
      this.removeEdge(edge)
    }
    for (edge <- this.getInEdges(vertex, Seq[String]())) {
      this.removeEdge(edge)
    }*/
    val delete = new Delete(vertex.id)
    table.delete(delete);
  }

  def getVertices() = null

  def getEdges() = null

  def getProperty(element: ElementT[Array[Byte]], key: String): Option[AnyRef] =
    element match {
      case
        v: VertexT[IdType] => {
        val rowGet = new Get(element.id)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This vertex does not exist");
        val p = row.getValue(vertexPropertiesColumnFamily, key)
        if (p != null)
          Some(fromTypedBytes(p))
        else
          None
      }
      case
        e: EdgeT[IdType] => {
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val rowGet = new Get(struct._1)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This edge does not exist")
        val p = row.getValue(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId(key, struct._2))
        if (p != null)
          Some(fromTypedBytes(p))
        else
          None
      }
    }

  def getPropertyKeys(element: ElementT[Array[Byte]]): Set[String] =
    element match {
      case
        v: VertexT[IdType] => {
        val rowGet = new Get(element.id)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This vertex does not exist")

        val familyMap = row.getFamilyMap(vertexPropertiesColumnFamily)
        (for {
          bkey <- familyMap.keySet.asScala
          key: String = bkey
          if (bkey.length != 0)
        } yield key).toSet
      }
      case
        e: EdgeT[IdType] => {
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val rowGet = new Get(struct._1)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This edge does not exist")

        val familyMap = row.getFamilyMap(edgePropertiesColumnFamily)

        (for {
          edgeProperty <- familyMap.asScala
          (key, edgeLocalId) = idGenerationStrategy.getEdgePropertyIdStruct(edgeProperty._1)
          if (edgeProperty.length != 0 && edgeLocalId == struct._2 && !(key == "label"))
        } yield key).toSet
      }
    }

  def removeProperty(element: ElementT[Array[Byte]], key: String) = null

  def setProperty(element: ElementT[Array[Byte]], key: String, obj: AnyRef) {}

  def getInEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]) = null

  def getOutEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]) = null
}