package eu.stratosphere.peel.core.cli.command.experiment

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.experiment.ExperimentSuite
import eu.stratosphere.peel.core.beans.system.{Lifespan, System}
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.{Configurable, loadConfig}
import eu.stratosphere.peel.core.graph.createGraph
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext

class Run extends Command {

  override def name() = "exp:run"

  override def help() = "execute a specific experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--fixtures")
      .`type`(classOf[String])
      .dest("app.path.fixtures")
      .metavar("FIXTURES")
      .help("fixtures file")
    parser.addArgument("--just")
      .`type`(classOf[Boolean])
      .dest("app.suite.experiment.just")
      .action(Arguments.storeTrue)
      .help("skip system set-up and tear-down")
    parser.addArgument("--run")
      .`type`(classOf[Integer])
      .dest("app.suite.experiment.run")
      .metavar("RUN")
      .help("run to execute")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("suite containing the experiment")
    parser.addArgument("experiment")
      .`type`(classOf[String])
      .dest("app.suite.experiment.name")
      .metavar("EXPERIMENT")
      .help("experiment to run")

    // option defaults
    parser.setDefault("app.path.fixtures", "config/fixtures.xml")
    parser.setDefault("app.suite.experiment.run", 1)
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.path.fixtures", Paths.get(ns.getString("app.path.fixtures")).normalize.toAbsolutePath.toString)
    Sys.setProperty("app.suite.experiment.just", if (ns.getBoolean("app.suite.experiment.just")) "true" else "false")
    Sys.setProperty("app.suite.experiment.run", ns.getInt("app.suite.experiment.run").toString)
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.suite.experiment.name", ns.getString("app.suite.experiment.name"))
  }

  override def run(context: ApplicationContext) = {
    val suiteName = Sys.getProperty("app.suite.suite.name")
    val expName = Sys.getProperty("app.suite.experiment.name")
    val expRun = Sys.getProperty("app.suite.experiment.run").toInt
    val justRun = Sys.getProperty("app.suite.experiment.just") == "true"

    logger.info(s"Running experiment '${Sys.getProperty("app.suite.experiment.name")}' from suite '${Sys.getProperty("app.suite.name")}'")

    val suite = context.getBean(Sys.getProperty("app.suite.name"), classOf[ExperimentSuite])
    val graph = createGraph(suite)

    //TODO check for cycles in the graph
    if (graph.isEmpty) throw new RuntimeException("Experiment suite is empty!")

    // find experiment
    val exps = suite.experiments.filter(_.name == expName)
    // load config
    for (e <- suite.experiments.filter(_.name == expName)) e.config = loadConfig(graph, e)
    // check if experiment exists (the list should contain exactly one element)
    if (exps.size != 1) throw new RuntimeException(s"Experiment '$expName' either not found or ambigous in suite '$suiteName'")

    for (exp <- exps; r <- Some(exp.run(expRun, force = true))) {
      try {
        logger.info("Executing experiment '%s'".format(exp.name))

        // update config
        for (n <- graph.descendants(exp)) n match {
          case s: Configurable => s.config = exp.config
          case _ => Unit
        }

        if (!justRun) {
          logger.info("Setting up systems with SUITE or EXPERIMENT lifespan")
          for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) n match {
            case s: System if (Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan) && !s.isUp => s.setUp()
            case _ => Unit
          }

          logger.info("Updating systems with PROVIDED lifespan")
          for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) n match {
            case s: System if Lifespan.PROVIDED :: Nil contains s.lifespan => s.update()
            case _ => Unit
          }
        } else {
          logger.info("Updating all systems")
          for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) n match {
            case s: System => s.update()
            case _ => Unit
          }
        }

        logger.info("Materializing experiment input data sets")
        for (n <- exp.inputs) n.materialize()

        logger.info("Tearing down redundant systems before conducting experiment runs")
        val required = graph.descendants(exp, exp.inputs).diff(Seq(exp)).toSet
        val redundant = graph.descendants(exp, required)
        for (n <- graph.traverse(); if redundant.contains(n)) n match {
          case s: System => s.tearDown()
          case _ => Unit
        }

        for (n <- exp.outputs) n.clean()
        r.execute() // run experiment
      } catch {
        case e: Throwable =>
          logger.error(s"Exception for experiment ${exp.name} in suite ${suite.name}: ${e.getMessage}")
          throw e

      } finally {
        if (!justRun) {
          logger.info("Tearing down systems with SUITE or EXPERIMENT lifespan")
          for (n <- graph.traverse(); if graph.descendants(exp).contains(n)) n match {
            case s: System if Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan => s.tearDown()
            case _ => Unit
          }
        }
      }
    }

  }
}
