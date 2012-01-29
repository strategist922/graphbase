package it.davidgreco.graphbase.core.impl

import collection.JavaConverters._
import java.io.IOException
import org.apache.hadoop.hbase._
import client._
import util.Bytes
import it.davidgreco.graphbase.core._
import collection.Iterable
import collection.immutable.{List, Set}
import java.util.{ArrayList, NavigableMap}

case class HBaseRepository(quorum: String, port: String, name: String) extends RepositoryT[Array[Byte]] {

  val idGenerationStrategy = BinaryRandomIdGenerationStrategy()

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


  createTables()

  private def createTables() {
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
      table = new HTable(admin.getConfiguration, tableName);
    } catch {
      case e: MasterNotRunningException => throw new RuntimeException(e)
      case e: ZooKeeperConnectionException => throw new RuntimeException(e)
      case e: IOException => throw new RuntimeException(e)
    }
  }

  private def dropTables() {
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

  def shutdown() {}

  def clear() {
    dropTables()
    createTables()
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

  private def generateRemoveEdgeDeletes(edge: EdgeT[Array[Byte]]): List[Delete] = {
    val outGet = new Get(edge.outVertex.id)
    val inGet = new Get(edge.inVertex.id)
    val rowOut = table.get(outGet)
    val rowIn = table.get(inGet)

    if (rowOut.isEmpty || rowIn.isEmpty) {
      throw new RuntimeException("One or both vertexes don't exist");
    }

    val edgeIdStruct: (Array[Byte], Array[Byte]) = idGenerationStrategy.getEdgeIdStruct(edge.id)

    val deleteOut = new Delete(rowOut.getRow)
    deleteOut.deleteColumns(outEdgesColumnFamily, edgeIdStruct._2);

    val deleteIn = new Delete(rowIn.getRow)
    deleteIn.deleteColumns(inEdgesColumnFamily, edgeIdStruct._2);

    deleteIn :: deleteOut :: edge.getPropertyKeys.flatMap(p => generateRemovePropertyDelete(edge, p)).map(p => p._1).toList
  }

  def removeEdge(edge: EdgeT[Array[Byte]]) {
    table.delete(new ArrayList[Delete](generateRemoveEdgeDeletes(edge).asJava))
  }

  def removeVertex(vertex: VertexT[Array[Byte]]) {
    val deleteVertex: Delete     = new Delete(vertex.id)
    val deletesOut: List[Delete] = this.getOutEdges(vertex, Seq[String]()).flatMap(e => this.generateRemoveEdgeDeletes(e)).toList
    val deletesIn: List[Delete]  = this.getInEdges(vertex, Seq[String]()).flatMap(e => this.generateRemoveEdgeDeletes(e)).toList
    table.delete(new ArrayList[Delete]((deleteVertex +: deletesIn ::: deletesOut).asJava))
  }

  def getVertices = {
    throw new UnsupportedOperationException
  }

  def getEdges = {
    throw new UnsupportedOperationException
  }

  def getProperty(element: ElementT[Array[Byte]], key: String): Option[AnyRef] =
    element match {
      case
        v: VertexT[IdType] => {
        val rowGet = new Get(element.id)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This vertex does not exist");
        val p = row.getValue(vertexPropertiesColumnFamily, toTypedBytes(key))
        Option(fromTypedBytes(p))
      }
      case
        e: EdgeT[IdType] => {
        val struct: (Array[Byte], Array[Byte]) = idGenerationStrategy.getEdgeIdStruct(element.id)
        val rowGet: Get = new Get(struct._1)
        val row: Result = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This edge does not exist")
        val p = row.getValue(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId(key, struct._2))
        Option(fromTypedBytes(p))
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
          key: String = if (bkey.length == 0) "" else fromTypedBytes(bkey)
          if (bkey != null && bkey.length != 0)
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
          if (!(key == "label"))
        } yield key).toSet
      }
    }

  private def generateRemovePropertyDelete(element: ElementT[Array[Byte]], key: String): Option[(Delete, Array[Byte])] =
    element match {
      case
        v: VertexT[IdType] => {
        val rowGet = new Get(v.id)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This vertex does not exist")
        val bvalue = row.getValue(vertexPropertiesColumnFamily, toTypedBytes(key))
        if (bvalue == null)
          return None
        val delete = new Delete(element.id);
        delete.deleteColumns(vertexPropertiesColumnFamily, key)
        Some((delete, bvalue))
      }
      case
        e: EdgeT[IdType] => {
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val rowGet = new Get(struct._1)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This edge does not exist");
        val ekey = idGenerationStrategy.generateEdgePropertyId(key, struct._2)
        val bvalue = row.getValue(edgePropertiesColumnFamily, ekey)
        if (bvalue == null)
          return None
        val delete = new Delete(struct._1);
        delete.deleteColumns(edgePropertiesColumnFamily, ekey)
        Some((delete, bvalue))
      }
    }


  def removeProperty(element: ElementT[Array[Byte]], key: String): Option[AnyRef] =
    for {
      (delete, value) <- generateRemovePropertyDelete(element, key)
    } yield {
      table.delete(delete)
      fromTypedBytes(value)
    }

  def setProperty(element: ElementT[Array[Byte]], key: String, value: AnyRef) {
    if (key == "id")
      throw new RuntimeException("Property with key 'id' is not allowed")
    element match {
      case
        v: VertexT[IdType] => {
        val rowGet = new Get(v.id)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This vertex does not exist")
        val put = new Put(v.id)
        put.add(vertexPropertiesColumnFamily, toTypedBytes(key), toTypedBytes(value))
        table.put(put)
      }
      case
        e: EdgeT[IdType] => {
        if (key == "label")
          throw new RuntimeException("Property with key 'label' is not allowed")
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val rowGet = new Get(struct._1)
        val row = table.get(rowGet)
        if (row.isEmpty)
          throw new RuntimeException("This edge does not exist");
        val put = new Put(struct._1)
        put.add(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId(key, struct._2), toTypedBytes(value))
        table.put(put)
      }
    }
  }

  def getInEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]): Iterable[EdgeT[IdType]] = {
    val rowGet = new Get(vertex.id)
    val row = table.get(rowGet)
    if (row.isEmpty)
      throw new RuntimeException("This vertex does not exist");
    val familyMap: NavigableMap[Array[Byte], Array[Byte]] = row.getFamilyMap(inEdgesColumnFamily)
    val edges = for {
      edge <- familyMap.asScala
      e = {
        val struct = idGenerationStrategy.getEdgeIdStruct(edge._2)
        val outVertex = CoreVertex(struct._1, this)
        val rowGet = new Get(struct._1)
        val row = table.get(rowGet)
        val label: String = row.getValue(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId("label", struct._2))
        new CoreEdge(edge._2, outVertex, vertex, label, this) with BinaryIdEquatable[CoreEdge[IdType]]
      }
      if ((labels.size != 0 && labels.contains(e.label)) || labels.size == 0)
    } yield e
    edges.toIterable.asInstanceOf[Iterable[EdgeT[IdType]]]
  }

  def getOutEdges(vertex: VertexT[Array[Byte]], labels: Seq[String]): Iterable[EdgeT[IdType]] = {
    val rowGet = new Get(vertex.id)
    val row = table.get(rowGet)
    if (row.isEmpty)
      throw new RuntimeException("This vertex does not exist");
    val familyMap: NavigableMap[Array[Byte], Array[Byte]] = row.getFamilyMap(outEdgesColumnFamily)
    val edges = for {
      edge <- familyMap.asScala
      e = {
        val vid: IdType = edge._2
        val id = idGenerationStrategy.generateEdgeId(vertex.id, edge._1)
        val inVertex = CoreVertex(vid, this)
        val label: String = row.getValue(edgePropertiesColumnFamily, idGenerationStrategy.generateEdgePropertyId("label", edge._1))
        new CoreEdge(id, vertex, inVertex, label, this) with BinaryIdEquatable[CoreEdge[IdType]]
      }
      if ((labels.size != 0 && labels.contains(e.label)) || labels.size == 0)
    } yield e
    edges.toIterable.asInstanceOf[Iterable[EdgeT[IdType]]]
  }
}