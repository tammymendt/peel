package eu.stratosphere.fab.extensions.beans.system.hadoop

import java.io.File
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import com.samskivert.mustache.Mustache
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.beans.system.{FileSystem, System}
import eu.stratosphere.fab.core.config.{Model, SystemConfig}
import eu.stratosphere.fab.core.util.Shell

class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("HDFS", lifespan, dependencies, mc) with FileSystem {

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'")

    if (config.hasPath("system.hadoop.path.archive")) {
      if (!Files.exists(Paths.get(config.getString("system.hadoop.path.home")))) {
        logger.info(s"Extracting archive ${config.getString("system.hadoop.path.archive.src")} to ${config.getString("system.hadoop.path.archive.dst")}")
        Shell.untar(config.getString("system.hadoop.path.archive.src"), config.getString("system.hadoop.path.archive.dst"))

        logger.info(s"Changing owner of ${config.getString("system.hadoop.path.home")} to ${config.getString("system.hadoop.user")}:${config.getString("system.hadoop.group")}")
        Shell.execute("chown -R %s:%s %s".format(
          config.getString("system.hadoop.user"),
          config.getString("system.hadoop.group"),
          config.getString("system.hadoop.path.home")))
      }
    }

    logger.info(s"Checking system configuration")
    configuration().update()

    if (config.getBoolean("system.hadoop.format")) format()

    Shell.execute(s"${config.getString("system.hadoop.path.home")}/bin/start-dfs.sh")
    logger.info(s"Waiting for safemode to exit")
    while (inSafemode) Thread.sleep(1000)
    logger.info(s"System '$toString' is now running")
  }

  override def tearDown(): Unit = {
    logger.info(s"Tearing down system '$toString'")

    Shell.execute(s"${config.getString("system.hadoop.path.home")}/bin/stop-dfs.sh", logOutput = true)

    if (config.getBoolean("system.hadoop.format")) format()
  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")

    val c = configuration()
    if (c.hasChanged) {
      logger.info(s"Configuration changed, restarting '$toString'...")
      Shell.execute(s"${config.getString("system.hadoop.path.home")}/bin/stop-dfs.sh", logOutput = true)

      if (config.getBoolean("system.hadoop.format")) format()

      c.update()

      if (config.getBoolean("system.hadoop.format")) format()

      Shell.execute(s"${config.getString("system.hadoop.path.home")}/bin/start-dfs.sh")
      logger.info(s"Waiting for safemode to exit")
      while (inSafemode) Thread.sleep(1000)
      logger.info(s"System '$toString' is now running")
    }
  }

  override def configuration() = SystemConfig(config, List(
    SystemConfig.Entry[Model.Hosts]("system.hadoop.config.masters",
      "%s/masters".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Hosts]("system.hadoop.config.slaves",
      "%s/slaves".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Env]("system.hadoop.config.env",
      "%s/hadoop-env.sh".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/hadoop-env.sh.mustache", mc),
    SystemConfig.Entry[Model.Site]("system.hadoop.config.core",
      "%s/core-site.xml".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/site.xml.mustache", mc),
    SystemConfig.Entry[Model.Site]("system.hadoop.config.hdfs",
      "%s/hdfs-site.xml".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/site.xml.mustache", mc)
  ))

  // ---------------------------------------------------
  // FileSystem.
  // ---------------------------------------------------

  /**
   * copies the given file to hdfs input directory and returns the path to
   * the file in hdfs
   * @param from path on local file system
   * @return path on hdfs
   */
  def setInput(from: File): File = {
    val to: File = new File("foobar") // new File(config.tring("path.hadoop.v1.input"), from.getName)
    //    logger.info("Copy Input data from %s to %s...".format(from, to))
    //    Shell.execute(home + "bin/hadoop fs -put %s %s".format(from, to), logOutput = true)
    to
  }

  //TODO check if file already exists - if so, remove?
  /**
   * retrieve data from hdfs output folder
   * copied the data from the hdfs output folder to a folder on
   * the local file system
   * @param to path on local fs where the data is copied to from hdfs
   * @return path on local fs for the output file
   */
  def getOutput(from: File, to: File) = {
    //    logger.info("Copy Input data from %s to %s...".format(from, to))
    //    Shell.execute(home + "bin/hadoop fs -get %s %s".format(from, to), logOutput = true)
  }

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  private def format() = {
    val user = config.getString("system.hadoop.user")
    val nameDir = config.getString("system.hadoop.config.hdfs.dfs.name.dir")

    logger.info(s"Formatting namenode")
    Shell.execute("echo 'Y' | %s/bin/hadoop namenode -format".format(config.getString("system.hadoop.path.home")))

    logger.info(s"Fixing data directories")
    for (dataNode <- config.getStringList("system.hadoop.config.slaves").asScala) {
      for (dataDir <- config.getString("system.hadoop.config.hdfs.dfs.data.dir").split(',')) {
        logger.info(s"Initializing data directory $dataDir at datanode $dataNode")
        Shell.execute( s""" ssh $user@$dataNode "rm -Rf $dataDir/current" """)
        Shell.execute( s""" ssh $user@$dataNode "mkdir -p $dataDir/current" """)
        logger.info(s"Copying namenode's VERSION file to datanode $dataNode")
        Shell.execute( s""" scp $nameDir/current/VERSION $user@$dataNode:$dataDir/current/VERSION.backup """)
        logger.info(s"Adapting VERSION file on datanode $dataNode")
        Shell.execute( s""" ssh $user@$dataNode "cat $dataDir/current/VERSION.backup | sed '3 i storageID=' | sed 's/storageType=NAME_NODE/storageType=DATA_NODE/g'" > $dataDir/current/VERSION """)
        Shell.execute( s""" ssh $user@$dataNode "rm -Rf $dataDir/current/VERSION.backup" """)
      }
    }
  }

  /**
   * Checks if HDFS is in safemode.
   *
   * @return
   */
  private def inSafemode: Boolean = {
    val msg = Shell.execute(s"${config.getString("system.hadoop.path.home")}/bin/hadoop dfsadmin -safemode get")
    val status = msg._1.toLowerCase
    !status.contains("off")
  }
}
