package eu.stratosphere.fab.extensions.beans.system.stratosphere

import java.io.File
import com.samskivert.mustache.Mustache
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.config.{Model, SystemConfig}
import java.nio.file.{Paths, Files}
import eu.stratosphere.fab.core.util.shell

class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends ExperimentRunner("Stratosphere", lifespan, dependencies, mc) {

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'")

    if (config.hasPath("system.stratosphere.path.archive")) {
      if (!Files.exists(Paths.get(config.getString("system.stratosphere.path.home")))) {
        logger.info(s"Extracting archive ${config.getString("system.stratosphere.path.archive.src")} to ${config.getString("system.stratosphere.path.archive.dst")}")
        shell.untar(config.getString("system.stratosphere.path.archive.src"), config.getString("system.stratosphere.path.archive.dst"))

        logger.info(s"Changing owner of ${config.getString("system.stratosphere.path.home")} to ${config.getString("system.stratosphere.user")}:${config.getString("system.stratosphere.group")}")
        shell ! "chown -R %s:%s %s".format(
          config.getString("system.stratosphere.user"),
          config.getString("system.stratosphere.group"),
          config.getString("system.stratosphere.path.home"))
      }
    }

    configuration().update()

    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/start-cluster.sh"
    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/start-webclient.sh"
    waitUntilAllTaskManagersRunning()
    logger.info(s"System '$toString' is now running")
  }

  override def configuration() = SystemConfig(config, List(
    SystemConfig.Entry[Model.Hosts]("system.stratosphere.config.slaves",
      "%s/slaves".format(config.getString("system.stratosphere.path.config")),
      "/templates/stratosphere/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Yaml]("system.stratosphere.config.yaml",
      "%s/stratosphere-conf.yaml".format(config.getString("system.stratosphere.path.config")),
      "/templates/stratosphere/conf/stratosphere-conf.yaml.mustache", mc)
  ))

  override def run(job: String, input: List[File], output: File) = {
    logger.info("Running Stratosphere Job...")
    //Shell ! home + "bin/stratosphere run %s %s %s".format(job, input.mkString(" "), output)
  }


  override def tearDown(): Unit = {
    logger.info(s"Tearing down system '$toString'")

    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-cluster.sh"
    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-webclient.sh"
  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")

    val c = configuration()
    if (c.hasChanged) {
      logger.info(s"Configuration changed, restarting '$toString'...")
      shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-cluster.sh"

      c.update()

      shell ! s"${config.getString("system.stratosphere.path.home")}/bin/start-cluster.sh"
      waitUntilAllTaskManagersRunning()
      logger.info(s"System '$toString' is now running")
    }
  }

  private def waitUntilAllTaskManagersRunning(): Unit = {
    val user = config.getString("system.stratosphere.user")
    val logDir = config.getString("system.stratosphere.path.log")

    val totl = config.getStringList("system.hadoop.config.slaves").size()
    val init = Integer.parseInt((shell !! s"""cat $logDir/stratosphere-$user-jobmanager-*.log | grep 'Creating instance' | wc -l""").trim())
    var curr = init
    while (curr - init < totl) {
      Thread.sleep(1000)
      curr = Integer.parseInt((shell !! s"""cat $logDir/stratosphere-$user-jobmanager-*.log | grep 'Creating instance' | wc -l""").trim())
    } // TODO: don't loop to infinity
  }
}
