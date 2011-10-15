package com.tinkerpop.blueprints.pgm.impls

import it.davidgreco.graphbase.core.{VertexT, EdgeT}
import com.tinkerpop.blueprints.pgm.{Edge, Vertex}

package object graphbase {

  implicit def edgeToGraphbaseEdge(edge: EdgeT): Edge = GraphbaseEdge(edge)

  implicit def vertexToGraphbaseVertex(vertex: VertexT): Vertex = GraphbaseVertex(vertex)

  implicit def graphbaseVertexToVertex(graphbaseVertex: Vertex): VertexT = graphbaseVertex.asInstanceOf[GraphbaseVertex].vertex

  implicit def graphbaseEdgeToEdge(graphbaseEdge: Edge): EdgeT = graphbaseEdge.asInstanceOf[GraphbaseEdge].edge

}