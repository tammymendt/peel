package eu.stratosphere.fab.core.context

import com.typesafe.config.Config
import eu.stratosphere.fab.core.graph.{DependencyGraph, Node}

case class ExecutionContext(graph: DependencyGraph[Node], config: Config)
