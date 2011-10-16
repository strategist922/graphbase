package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core._
import collection.mutable.ConcurrentMap
import collection.JavaConversions._
import java.util.concurrent.ConcurrentHashMap
import com.eaio.uuid.UUID

case class MemoryBasedRepository(name: String) extends RepositoryT {

  var table: ConcurrentMap[AnyRef, ConcurrentMap[AnyRef, ConcurrentMap[AnyRef, Array[Byte]]]] = new ConcurrentHashMap[AnyRef, ConcurrentMap[AnyRef, ConcurrentMap[AnyRef, Array[Byte]]]]

  def createVertex: VertexT = {
    val id = generateVertexId
    val row = new ConcurrentHashMap[AnyRef, ConcurrentMap[AnyRef, Array[Byte]]]
    row += "VERTEXPROPERTIES" -> new ConcurrentHashMap[AnyRef, Array[Byte]]()
    row += "OUTEDGES" -> new ConcurrentHashMap[AnyRef, Array[Byte]]()
    row += "EDGEPROPERTIES" -> new ConcurrentHashMap[AnyRef, Array[Byte]]()
    row += "INEDGES" -> new ConcurrentHashMap[AnyRef, Array[Byte]]()
    table += id -> row
    CoreVertex(id, this)
  }

  def createEdge(out: VertexT, in: VertexT, label: String): EdgeT = {
    val eli = generateEdgeLocalId
    val id = generateEdgeId(out.id, eli)
    var rowOut = table.get(out.id)
    var rowIn = table.get(in.id)
    if (!(rowIn.isDefined && rowOut.isDefined)) {
      throw new RuntimeException("One or both vertexes don't exist");
    }
    rowOut.get("OUTEDGES") += eli -> in.id
    rowOut.get("EDGEPROPERTIES") += generateEdgePropertyId("label", eli) -> label
    rowIn.get("INEDGES") += eli -> id
    CoreEdge(id, out, in, label, this)
  }

  def getVertex(id: AnyRef): Option[VertexT] = {
    if (id == null)
      return None;
    val row = table.get(id)
    if (!row.isDefined)
      None
    else {
      Some(CoreVertex(id, this))
    }
  }

  def getEdge(id: AnyRef): Option[EdgeT] = {
    if (id == null)
      return None;
    val struct = getEdgeIdStruct(id)
    val rowOut = table.get(struct._1)
    if (!rowOut.isDefined)
      return None
    var inId: Array[Byte] = null
    try {
      inId = rowOut.get.get("OUTEDGES").get(struct._2)
    }
    catch {
      case e: NoSuchElementException => return None
      case x => throw x
    }

    val inVertexId: String = inId
    val label: String = rowOut.get.get("EDGEPROPERTIES").get(generateEdgePropertyId("label", struct._2))
    val outVertex = CoreVertex(struct._1, this)
    val inVertex = CoreVertex(inVertexId, this)
    Some(CoreEdge(id, outVertex, inVertex, label, this))
  }

  def removeEdge(edge: EdgeT) = {
    val rowOut = table.get(edge.outVertex.id)
    val rowIn = table.get(edge.inVertex.id)
    if (!rowOut.isEmpty && !rowIn.isEmpty) {
      val struct = getEdgeIdStruct(edge.id)
      rowOut.get.get("OUTEDGES").get.remove(struct._2)
      rowIn.get.get("INEDGES").get.remove(struct._2)
    }
  }

  def removeVertex(vertex: VertexT) = {
    for (edge <- this.getOutEdges(vertex, Seq[String]())) {
      this.removeEdge(edge)
    }
    for (edge <- this.getInEdges(vertex, Seq[String]())) {
      this.removeEdge(edge)
    }
    table -= vertex.id
  }

  def getProperty(element: ElementT, key: String): Option[AnyRef] =
    element match {
      case
        v: VertexT => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        val p = row.get("VERTEXPROPERTIES").get(key)
        if (p.isDefined)
          Some(fromTypedBytes(p.get).asInstanceOf[AnyRef])
        else
          None
      }
      case
        e: EdgeT => {
        val struct = getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        val p = row.get("EDGEPROPERTIES").get(generateEdgePropertyId(key, struct._2))
        if (p.isDefined)
          Some(fromTypedBytes(p.get).asInstanceOf[AnyRef])
        else
          None
      }
    }

  def getPropertyKeys(element: ElementT): Set[String] =
    element match {
      case
        v: VertexT => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        (for {
          k <- row.get("VERTEXPROPERTIES").keys
          ks = k.asInstanceOf[String]
        } yield ks).toSet
      }
      case
        e: EdgeT => {
        val struct = getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        val edgeProperties = row.get("EDGEPROPERTIES")
        (for {
          edgeProperty <- edgeProperties
          (key, edgeLocalId) = getEdgePropertyIdStruct(edgeProperty._1)
          if (edgeLocalId.asInstanceOf[String] == struct._2.asInstanceOf[String] && !(key == "label"))
        } yield key).toSet
      }
    }

  def removeProperty(element: ElementT, key: String): Option[AnyRef] =
    element match {
      case
        v: VertexT => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        val p = row.get("VERTEXPROPERTIES").get(key)
        if (p.isDefined) {
          row.get("VERTEXPROPERTIES").remove(key)
          Some(fromTypedBytes(p.get).asInstanceOf[AnyRef])
        }
        else
          None
      }
      case
        e: EdgeT => {
        val struct = getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        val ekey = generateEdgePropertyId(key, struct._2).asInstanceOf[String]
        val p = row.get("EDGEPROPERTIES").get(ekey)
        if (p.isDefined) {
          row.get("EDGEPROPERTIES").remove(ekey)
          Some(fromTypedBytes(p.get).asInstanceOf[AnyRef])
        }
        else
          None
      }
    }

  def setProperty(element: ElementT, key: String, value: AnyRef): Unit = {
    if (key == "id")
      throw new RuntimeException("Property with key 'id' is not allowed")
    element match {
      case
        v: VertexT => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        row.get("VERTEXPROPERTIES") += key -> toTypedBytes(value)
      }
      case
        e: EdgeT => {
        if (key == "label")
          throw new RuntimeException("Property with key 'label' is not allowed")
        val struct = getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        row.get("EDGEPROPERTIES") += generateEdgePropertyId(key, struct._2).asInstanceOf[String] -> toTypedBytes(value)
      }
    }
  }

  def getInEdges(vertex: VertexT, labels: Seq[String]): Iterable[EdgeT] = {
    val row = table.get(vertex.id)
    if (!row.isDefined) {
      throw new RuntimeException("The vertex does not exist");
    }
    val edges = for {
      edge: (AnyRef, Array[Byte]) <- row.get("INEDGES")
      e = {
        val eid: String = edge._2
        val struct = getEdgeIdStruct(eid)
        val outVertex = CoreVertex(struct._1, this)
        val label: String = table.get(struct._1).get("EDGEPROPERTIES").get(generateEdgePropertyId("label", edge._1)).get
        val edgeId: String = edge._2
        CoreEdge(edgeId, outVertex, vertex, label, this)
      }
      if ((labels.size != 0 && labels.contains(e.label)) || labels.size == 0)
    } yield e
    edges.toIterable.asInstanceOf[Iterable[EdgeT]]
  }

  def getOutEdges(vertex: VertexT, labels: Seq[String]): Iterable[EdgeT] = {
    val row = table.get(vertex.id)
    if (!row.isDefined) {
      throw new RuntimeException("The vertex does not exist");
    }
    val edges = for {
      edge: (AnyRef, Array[Byte]) <- row.get("OUTEDGES")
      e = {
        val vid: String = edge._2
        val id = generateEdgeId(vertex.id, edge._1)
        val inVertex = CoreVertex(vid, this)
        val label: String = row.get("EDGEPROPERTIES").get(generateEdgePropertyId("label", edge._1)).get
        CoreEdge(id, vertex, inVertex, label, this)
      }
      if ((labels.size != 0 && labels.contains(e.label)) || labels.size == 0)
    } yield e
    edges.toIterable.asInstanceOf[Iterable[EdgeT]]
  }

  def shutdown() {
  }

  def clear() = {
    table = new ConcurrentHashMap[AnyRef, ConcurrentMap[AnyRef, ConcurrentMap[AnyRef, Array[Byte]]]]
  }

  def generateVertexId: AnyRef = {
    val rid: UUID = new UUID
    rid.toString
  }

  def generateEdgeLocalId: AnyRef = {
    val rid: UUID = new UUID
    rid.getTime.toHexString
  }

  def generateEdgeId(vertexId: AnyRef, edgeLocalId: AnyRef): AnyRef = vertexId.asInstanceOf[String] + '#' + edgeLocalId.asInstanceOf[String]

  def generateEdgePropertyId(propertyKey: String, edgeLocalId: AnyRef): AnyRef = propertyKey + '#' + edgeLocalId.asInstanceOf[String]

  def getEdgeIdStruct(edgeId: AnyRef): (AnyRef, AnyRef) = {
    val fields = edgeId.asInstanceOf[String].split('#')
    (fields.apply(0), fields.apply(1))
  }

  def getEdgePropertyIdStruct(edgePropertyId: AnyRef): (String, AnyRef) = {
    val fields = edgePropertyId.asInstanceOf[String].split('#')
    (fields.apply(0).asInstanceOf[String], fields.apply(1))
  }
}