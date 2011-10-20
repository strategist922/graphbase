package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core._
import collection.mutable.ConcurrentMap
import collection.JavaConversions._
import java.util.concurrent.ConcurrentHashMap

case class MemoryBasedRepository(name: String, idGenerationStrategy: IdGenerationStrategyT[String]) extends RepositoryT[String] with WithIdGenerationStrategyT[String] {

  var table: ConcurrentMap[IdType, ConcurrentMap[IdType, ConcurrentMap[IdType, Array[Byte]]]] = new ConcurrentHashMap[IdType, ConcurrentMap[IdType, ConcurrentMap[IdType, Array[Byte]]]]

  def createVertex: VertexT[IdType] = {
    val id = idGenerationStrategy.generateVertexId
    val row = new ConcurrentHashMap[IdType, ConcurrentMap[IdType, Array[Byte]]]
    row += "VERTEXPROPERTIES" -> new ConcurrentHashMap[IdType, Array[Byte]]()
    row += "OUTEDGES" -> new ConcurrentHashMap[IdType, Array[Byte]]()
    row += "EDGEPROPERTIES" -> new ConcurrentHashMap[IdType, Array[Byte]]()
    row += "INEDGES" -> new ConcurrentHashMap[IdType, Array[Byte]]()
    table += id -> row
    CoreVertex(id, this)
  }

  def createEdge(out: VertexT[IdType], in: VertexT[IdType], label: String): EdgeT[IdType] = {
    val eli = idGenerationStrategy.generateEdgeLocalId
    val id = idGenerationStrategy.generateEdgeId(out.id, eli)
    var rowOut = table.get(out.id)
    var rowIn = table.get(in.id)
    if (!(rowIn.isDefined && rowOut.isDefined)) {
      throw new RuntimeException("One or both vertexes don't exist");
    }
    rowOut.get("OUTEDGES") += eli -> in.id
    rowOut.get("EDGEPROPERTIES") += idGenerationStrategy.generateEdgePropertyId("label", eli) -> label
    rowIn.get("INEDGES") += eli -> id
    CoreEdge(id, out, in, label, this)
  }

  def getVertex(id: IdType): Option[VertexT[IdType]] = {
    if (id == null)
      return None;
    val row = table.get(id)
    if (!row.isDefined)
      None
    else {
      Some(CoreVertex(id, this))
    }
  }

  def getEdge(id: IdType): Option[EdgeT[IdType]] = {
    if (id == null)
      return None;
    val struct = idGenerationStrategy.getEdgeIdStruct(id)
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

    val label: String = rowOut.get.get("EDGEPROPERTIES").get(idGenerationStrategy.generateEdgePropertyId("label", struct._2))
    val outVertex = CoreVertex[IdType](struct._1, this)
    val inVertex = CoreVertex[IdType](inId, this)
    Some(CoreEdge[IdType](id, outVertex, inVertex, label, this))
  }

  def removeEdge(edge: EdgeT[IdType]) = {
    val rowOut = table.get(edge.outVertex.id)
    val rowIn = table.get(edge.inVertex.id)
    if (!rowOut.isEmpty && !rowIn.isEmpty) {
      val struct = idGenerationStrategy.getEdgeIdStruct(edge.id)
      rowOut.get.get("OUTEDGES").get.remove(struct._2)
      rowIn.get.get("INEDGES").get.remove(struct._2)
    }
  }

  def removeVertex(vertex: VertexT[IdType]) = {
    for (edge <- this.getOutEdges(vertex, Seq[String]())) {
      this.removeEdge(edge)
    }
    for (edge <- this.getInEdges(vertex, Seq[String]())) {
      this.removeEdge(edge)
    }
    table -= vertex.id
  }

  def getProperty(element: ElementT[IdType], key: String): Option[AnyRef] =
    element match {
      case
        v: VertexT[IdType] => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        val p = row.get("VERTEXPROPERTIES").get(key)
        if (p.isDefined)
          Some(fromTypedBytes(p.get))
        else
          None
      }
      case
        e: EdgeT[IdType] => {
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        val p = row.get("EDGEPROPERTIES").get(idGenerationStrategy.generateEdgePropertyId(key, struct._2))
        if (p.isDefined)
          Some(fromTypedBytes(p.get))
        else
          None
      }
    }

  def getPropertyKeys(element: ElementT[IdType]): Set[String] =
    element match {
      case
        v: VertexT[IdType] => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        (for {
          k <- row.get("VERTEXPROPERTIES").keys
        } yield k).toSet
      }
      case
        e: EdgeT[IdType] => {
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        val edgeProperties = row.get("EDGEPROPERTIES")
        (for {
          edgeProperty <- edgeProperties
          (key, edgeLocalId) = idGenerationStrategy.getEdgePropertyIdStruct(edgeProperty._1)
          if (edgeLocalId == struct._2 && !(key == "label"))
        } yield key).toSet
      }
    }

  def removeProperty(element: ElementT[IdType], key: String): Option[AnyRef] =
    element match {
      case
        v: VertexT[IdType] => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        val p = row.get("VERTEXPROPERTIES").get(key)
        if (p.isDefined) {
          row.get("VERTEXPROPERTIES").remove(key)
          Some(fromTypedBytes(p.get))
        }
        else
          None
      }
      case
        e: EdgeT[IdType] => {
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        val ekey = idGenerationStrategy.generateEdgePropertyId(key, struct._2)
        val p = row.get("EDGEPROPERTIES").get(ekey)
        if (p.isDefined) {
          row.get("EDGEPROPERTIES").remove(ekey)
          Some(fromTypedBytes(p.get))
        }
        else
          None
      }
    }

  def setProperty(element: ElementT[IdType], key: String, value: AnyRef): Unit = {
    if (key == "id")
      throw new RuntimeException("Property with key 'id' is not allowed")
    element match {
      case
        v: VertexT[IdType] => {
        val row = table.get(element.id)
        if (row == null)
          throw new RuntimeException("This vertex does not exist");
        row.get("VERTEXPROPERTIES") += key -> value
      }
      case
        e: EdgeT[IdType] => {
        if (key == "label")
          throw new RuntimeException("Property with key 'label' is not allowed")
        val struct = idGenerationStrategy.getEdgeIdStruct(element.id)
        val row = table.get(struct._1)
        if (row == null)
          throw new RuntimeException("This edge does not exist");
        row.get("EDGEPROPERTIES") += idGenerationStrategy.generateEdgePropertyId(key, struct._2) -> toTypedBytes(value)
      }
    }
  }

  def getInEdges(vertex: VertexT[IdType], labels: Seq[String]): Iterable[EdgeT[IdType]] = {
    val row = table.get(vertex.id)
    if (!row.isDefined) {
      throw new RuntimeException("The vertex does not exist");
    }
    val edges = for {
      edge: (IdType, Array[Byte]) <- row.get("INEDGES")
      e = {
        val eid: IdType = edge._2
        val struct = idGenerationStrategy.getEdgeIdStruct(eid)
        val outVertex = CoreVertex(struct._1, this)
        val label: String = table.get(struct._1).get("EDGEPROPERTIES").get(idGenerationStrategy.generateEdgePropertyId("label", edge._1)).get
        CoreEdge(eid, outVertex, vertex, label, this)
      }
      if ((labels.size != 0 && labels.contains(e.label)) || labels.size == 0)
    } yield e
    edges.toIterable.asInstanceOf[Iterable[EdgeT[IdType]]]
  }

  def getOutEdges(vertex: VertexT[IdType], labels: Seq[String]): Iterable[EdgeT[IdType]] = {
    val row = table.get(vertex.id)
    if (!row.isDefined) {
      throw new RuntimeException("The vertex does not exist");
    }
    val edges = for {
      edge: (IdType, Array[Byte]) <- row.get("OUTEDGES")
      e = {
        val vid: IdType = edge._2
        val id = idGenerationStrategy.generateEdgeId(vertex.id, edge._1)
        val inVertex = CoreVertex(vid, this)
        val label: String = row.get("EDGEPROPERTIES").get(idGenerationStrategy.generateEdgePropertyId("label", edge._1)).get
        CoreEdge(id, vertex, inVertex, label, this)
      }
      if ((labels.size != 0 && labels.contains(e.label)) || labels.size == 0)
    } yield e
    edges.toIterable.asInstanceOf[Iterable[EdgeT[IdType]]]
  }

  def getVertices(): Iterable[VertexT[IdType]] = {
    (for {
      row <- table
      v: VertexT[IdType] = getVertex(row._1).get
    } yield v).toList
  }

  def getEdges(): Iterable[EdgeT[IdType]] = {
    (for {
      row <- table
      e: EdgeT[IdType] <- getVertex(row._1).get.getOutEdges(Seq()).toList
    } yield e).toList

  }

  def shutdown(): Unit = {
  }

  def clear(): Unit = {
    table = new ConcurrentHashMap[IdType, ConcurrentMap[IdType, ConcurrentMap[IdType, Array[Byte]]]]
  }

}