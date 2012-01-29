package it.davidgreco.graphbase.core.impl

import it.davidgreco.graphbase.core.GraphT

case class HBaseGraph(repository: HBaseRepository) extends GraphT[Array[Byte]]