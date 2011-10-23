package it.davidgreco.graphbase.core.impl

import collection.JavaConverters._
import it.davidgreco.graphbase.core._
import java.io.IOException
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, MasterNotRunningException, ZooKeeperConnectionException}
import org.apache.hadoop.hbase.client._

case class HBaseRepository(admin: HBaseAdmin, name: String, idGenerationStrategy: IdGenerationStrategyT[Array[Byte]]) extends RepositoryT[Array[Byte]] {

  private val tableName = name + '$' + "GRAPH"
  private val vertexPropertiesColumnFamily: Array[Byte] = tableName + '$' + "VERTEXPROPERTIES"
  private val edgePropertiesColumnFamily: Array[Byte] = tableName + '$' + "EDGEPROPERTIES"
  private val inEdgesColumnFamily: Array[Byte] = tableName + '$' + "INEDGES"
  private val outEdgesColumnFamily: Array[Byte] = tableName + '$' + "OUTEDGES"
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
    put.add(vertexPropertiesColumnFamily, null, null);
    table.put(put);

    CoreVertex(id, this)
  }

  def createEdge(out: VertexT[Array[Byte]], in: VertexT[Array[Byte]], label: String): EdgeT[IdType] = {
    val eli = idGenerationStrategy.generateEdgeLocalId
    val id = idGenerationStrategy.generateEdgeId(out.id, eli)

    val outGet = new Get(out.id)
    val inGet = new Get(in.id)
    val rowOut = table.get(outGet)
    val rowIn = table.get(inGet)

    if (rowOut.isEmpty || rowIn.isEmpty) {
      throw new RuntimeException("One or both vertexes don't exist");
    }

    val outPut = new Put(out.id)
    outPut.add(outEdgesColumnFamily, eli, in.id)
    val blabel: Array[Byte] = label
    outPut.add(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId("label", eli), blabel)

    val inPut = new Put(in.id)
    inPut.add(inEdgesColumnFamily, eli, id)

    table.put(List(outPut, inPut).asJava)

    CoreEdge(id, out, in, label, this)
  }

  def getVertex(id: Array[Byte]): Option[VertexT[IdType]] = {
    if (id == null)
      return None;
    val rowGet = new Get(id)
    val row = table.get(rowGet)
    if (row.isEmpty)
      None
    else {
      Some(CoreVertex(id, this))
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

    var inId: Array[Byte] = rowOut.getValue(outEdgesColumnFamily, struct._2)
    if (inId == null)
      return None

    val label: String = rowOut.getValue(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId("label", struct._2))

    val outVertex = CoreVertex[IdType](struct._1, this)
    val inVertex = CoreVertex[IdType](inId, this)
    Some(CoreEdge[IdType](id, outVertex, inVertex, label, this))
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