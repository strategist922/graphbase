package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core.GraphT

case class MemoryBasedGraph(repository: MemoryBasedRepository) extends GraphT[String]
