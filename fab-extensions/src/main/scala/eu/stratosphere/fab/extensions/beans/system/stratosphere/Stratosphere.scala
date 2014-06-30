package eu.stratosphere.fab.extensions.beans.system.stratosphere

import java.io.File
import com.samskivert.mustache.Mustache
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.config.{Model, SystemConfig}
import java.nio.file.{Paths, Files}
import eu.stratosphere.fab.core.util.Shell

class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends ExperimentRunner("Stratosphere", lifespan, dependencies, mc) {

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'")

    if (config.hasPath("system.hadoop.paths.archive")) {
      if (!Files.exists(Paths.get(config.getString("system.hadoop.paths.home")))) {
        logger.info(s"Extracting archive ${config.getString("system.hadoop.paths.archive.src")} to ${config.getString("system.hadoop.paths.archive.dst")}")
        Shell.untar(config.getString("system.hadoop.paths.archive.src"), config.getString("system.hadoop.paths.archive.dst"))

        logger.info(s"Changing owner of ${config.getString("system.hadoop.paths.home")} to ${config.getString("system.hadoop.user")}:${config.getString("system.hadoop.group")}")
        Shell.execute("chown -R %s:%s %s".format(
          config.getString("system.hadoop.user"),
          config.getString("system.hadoop.group"),
          config.getString("system.hadoop.paths.home")))
      }
    }
    logger.info(s"Checking system configuration")

    configuration().update()

    Shell.execute(s"${config.getString("system.stratosphere.paths.home")}/bin/start-cluster.sh")
    logger.info(s"System '$toString' is now running")
  }

  override def configuration() = SystemConfig(config, List(
    SystemConfig.Entry[Model.Hosts]("system.stratosphere.config.slaves",
      "%s/slaves".format(config.getString("system.stratosphere.paths.config")),
      "/templates/stratosphere/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Site]("system.stratosphere.config.yaml",
      "%s/stratosphere-conf.yaml".format(config.getString("system.stratosphere.paths.config")),
      "/templates/stratosphere/conf/stratosphere-conf.yaml.mustache", mc)
  ))

  override def run(job: String, input: List[File], output: File) = {
    logger.info("Running Stratosphere Job...")
    //Shell.execute(home + "bin/stratosphere run %s %s %s".format(job, input.mkString(" "), output), true)
  }


  override def tearDown(): Unit = {
    logger.info(s"Tearing down '$toString'")

    Shell.execute(s"${config.getString("system.stratosphere.paths.home")}/bin/stop-cluster.sh")

  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")

    val c = configuration()
    if (c.hasChanged) {
      logger.info(s"Configuration changed, restarting '$toString'...")
      Shell.execute(s"${config.getString("system.stratosphere.paths.home")}/bin/stop-cluster.sh")

      c.update()

      Shell.execute(s"${config.getString("system.stratosphere.paths.home")}/bin/start-cluster.sh")
      logger.info(s"System '$toString' is now running")
    }
  }


}
