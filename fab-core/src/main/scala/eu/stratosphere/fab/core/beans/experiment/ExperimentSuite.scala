package eu.stratosphere.fab.core.beans.experiment

import com.typesafe.config.ConfigFactory
import eu.stratosphere.fab.core.beans.system.{Lifespan, System}
import eu.stratosphere.fab.core.context.ExecutionContext
import eu.stratosphere.fab.core.graph.{Node, DependencyGraph}
import org.slf4j.LoggerFactory

class ExperimentSuite(final val experiments: List[Experiment]) extends Node {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def run() = {

    logger.info("Constructing dependency graph for suite")
    val graph = createGraph()

    //TODO check for cycles in the graph
    if (graph.isEmpty)
      throw new RuntimeException("Suite is empty!")

    val ctx = ExecutionContext(graph, loadConfig(graph))

    for (n <- ctx.graph.reverse.dfs()) n match {
      case s: System => s.config = Some(ctx.config)
      case _ => Unit
    }

    try {
      logger.info("Setting up systems with SUITE lifecycle")
      setUp(ctx)
      logger.info("Executing experiments in suite")
      Thread.sleep(20000)
      //      for (exp <- experiments) exp.run(ctx)
    } catch {
      case e: Exception => logger.error(s"Exception of type ${e.getClass} in ExperimentSuite: ${e.getMessage}")
      case _: Throwable => logger.error(s"Exception in ExperimentSuite")
    } finally {
      logger.info("Tearing down systems with SUITE lifecycle")
      tearDown(ctx)
    }
  }

  private def loadConfig(graph: DependencyGraph[Node]) = {
    var config = ConfigFactory.load()
    for (n <- graph.reverse.dfs()) n match {
      case s: System =>
        config = ConfigFactory.load(s.name).withFallback(
          if (s.name != s.defaultName)
            ConfigFactory.load(s.defaultName).withFallback(config)
          else
            config)
      case _ => Unit
    }
    config
  }

  /**
   * Set up all systems with SUITE lifespan.
   *
   * @param ctx The execution context.
   */
  private def setUp(ctx: ExecutionContext) = {
    for (n <- ctx.graph.reverse.dfs()) n match {
      case s: System => if (s.lifespan == Lifespan.SUITE) s.setUp()
      case _ => Unit
    }
  }

  /**
   * Tear down all dependencies with SUITE lifespan.
   *
   * @param ctx The execution context.
   */
  private def tearDown(ctx: ExecutionContext) = {
    for (n <- ctx.graph.reverse.dfs().reverse) n match {
      case s: System => if (s.lifespan == Lifespan.SUITE) s.tearDown()
      case _ => Unit
    }
  }

  /**
   * Create a directed Graph from all Experiments and their dependencies.
   *
   * @return Graph with systems as vertices and dependencies as edges
   */
  private def createGraph(): DependencyGraph[Node] = {

    val g = new DependencyGraph[Node]

    def processDependencies(s: System): Unit = {
      if (s.dependencies.nonEmpty) {
        for (d <- s.dependencies) {
          g.addEdge(s, d)
          processDependencies(d)
        }
      }
    }

    for (e <- experiments) {
      g.addEdge(this, e)
      g.addEdge(e, e.runner)
      processDependencies(e.runner)
    }

    g // return the graph
  }

  override def toString = "Experiment Suite"
}
