package it.davidgreco.graphbase.core

case class CoreEdge(id: AnyRef, outVertex: VertexT, inVertex: VertexT, label: String, repository: RepositoryT) extends EdgeT