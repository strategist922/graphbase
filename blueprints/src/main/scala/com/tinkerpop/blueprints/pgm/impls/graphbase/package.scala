package com.tinkerpop.blueprints.pgm.impls

import it.davidgreco.graphbase.core.{VertexT, EdgeT}
import com.tinkerpop.blueprints.pgm.{Edge, Vertex}

package object graphbase {

  implicit def edgeToGraphbaseEdge[T](edge: EdgeT[T]): Edge = GraphbaseEdge[T](edge)

  implicit def vertexToGraphbaseVertex[T](vertex: VertexT[T]): Vertex = GraphbaseVertex[T](vertex)

  implicit def graphbaseVertexToVertex[T](graphbaseVertex: Vertex): VertexT[T] = graphbaseVertex.asInstanceOf[GraphbaseVertex[T]].vertex

  implicit def graphbaseEdgeToEdge[T](graphbaseEdge: Edge): EdgeT[T] = graphbaseEdge.asInstanceOf[GraphbaseEdge[T]].edge

}