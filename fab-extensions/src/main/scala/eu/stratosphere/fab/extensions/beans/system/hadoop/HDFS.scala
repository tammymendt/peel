package eu.stratosphere.fab.extensions.beans.system.hadoop

import com.github.mustachejava.MustacheFactory
import eu.stratosphere.fab.core.beans.system.{FileSystem, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.config.namevalue
import eu.stratosphere.fab.core.util.Shell
import java.io.{PrintWriter, File}

import scala.collection.mutable.ListBuffer

class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set(), mf: MustacheFactory) extends System("HDFS", lifespan, dependencies, mf) with FileSystem {

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  /**
   * setup hdfs
   *
   * Creates a complete hdfs installation with configuration
   * and starts a single node cluster
   */
  //TODO: allow for more nodes
  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'")
    logger.info(s"Extracting archive ${config.get.getString("system.hadoop.paths.archive.src")} to ${config.get.getString("system.hadoop.paths.archive.dst")}")
    Shell.untar(config.get.getString("system.hadoop.paths.archive.src"), config.get.getString("system.hadoop.paths.archive.dst"))

    logger.info(s"Changing owner of ${config.get.getString("system.hadoop.paths.home")} to ${config.get.getString("app.system.user")}")
    Shell.execute(("chown -R %s:%s %s").format(
      config.get.getString("app.system.user"),
      config.get.getString("app.system.group"),
      config.get.getString("system.hadoop.paths.home")))

    configure()
    if (config.get.getBoolean("system.hadoop.namenode.format")) {
      logger.info(s"Formatting namenode")
      Shell.execute("%s/bin/hadoop namenode -format -force".format(config.get.getString("system.hadoop.paths.home")))
    }

    //    Shell.execute(s"${config.get.getString("system.hadoop.paths.home")}/bin/start-dfs.sh")
    //    logger.info(s"Waiting for safemode to exit...")
    //    while (inSafemode) Thread.sleep(500)
    //    logger.info(s"System '$toString' is now running")
  }

  /**
   * tear down hdfs
   *
   * Shuts down NameNode, Jobtracker and all other Nodes
   */
  //TODO remove data folders
  override def tearDown(): Unit = {
    //    logger.info(s"Tearing down system '$toString'...")
    //    Shell.execute(s"${config.get.getString("system.hadoop.paths.home")}/bin/stop-dfs.sh", logOutput = true)
  }

  /**
   * updates hdfs
   * the system is shut down and set up again with different parameters
   *
   * TODO implement good way to update the parameters
   * - give config object as parameter that can be used in setup and configure
   */
  override def update(): Unit = {
    logger.info(s"Updating system '$toString'...")
    //    tearDown(config: Config)
    //    setUp(config: Config)
  }

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
    val to: File = new File("foobar") // new File(config.getString("paths.hadoop.v1.input"), from.getName)
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

  /**
   * Checks if HDFS is in safemode.
   *
   * @return
   */
  private def inSafemode: Boolean = {
    val msg = Shell.execute(s"${config.get.getString("system.hadoop.paths.home")}/bin/hadoop dfsadmin -safemode get")
    val status = msg._1.toLowerCase
    !status.contains("off")
  }

  /**
   * Configures HDFS.
   *
   * the necessary config files are created with the given configuration
   * if there are several runs, a new config file can be passed and new
   * configuration files in the hadoop conf folder are created (overwritten)
   */
  def configure() = {
    logger.info(s"Configuring " + toString + "...")

    val siteTemplate = mf.compile("hadoop/conf/site.xml.mustache")
    val envTemplate = mf.compile("hadoop/conf/hadoop-env.sh.mustache")

    //envTemplate.execute(new PrintWriter(System.out), new namevalue.Context(config.get, "system.hadoop.config.env")).flush()
    val coreSiteFile = new File(s"${config.get.getString("system.hadoop.paths.config")}/core-site.xml")
    val hdfsSiteFile = new File(s"${config.get.getString("system.hadoop.paths.config")}/core-site.xml")

    siteTemplate.execute(new PrintWriter(coreSiteFile), new namevalue.Context(config.get, "system.hadoop.config.core-site")).flush()
    siteTemplate.execute(new PrintWriter(hdfsSiteFile), new namevalue.Context(config.get, "system.hadoop.config.hdfs-site")).flush()

    Unit

    //    val st = new ST("Hello, <name>")
    //    st.add

    //    // hadoop-env
    //    var names: List[String] = conf.getStringList("hadoop.v1.hadoop-env.names").asScala.toList
    //    var values: List[String] = conf.getStringList("hadoop.v1.hadoop-env.values").asScala.toList
    //    val envString = envTemplate(names, values)
    //    printToFile(new File(configPath + "hadoop-env.sh"))(p => {
    //      envString.foreach(p.println)
    //    })
    //
    //    // core-site.xml
    //    names = conf.getStringList("hdfs.v1.core-site.names").asScala.toList
    //    values = conf.getStringList("hdfs.v1.core-site.values").asScala.toList
    //    var confString = configTemplate(names, values)
    //    scala.xml.XML.save(s"${config.get.getString("system.hadoop.paths.config")}/core-site.xml", confString, "UTF-8", xmlDecl = true, null)
    //
    //    // hdfs-site.xml
    //    names = conf.getStringList("hdfs.v1.hdfs-site.names").asScala.toList
    //    values = conf.getStringList("hdfs.v1.hdfs-site.values").asScala.toList
    //    confString = configTemplate(names, values)
    //    scala.xml.XML.save(configPath + "hdfs-site.xml", confString, "UTF-8", logOutput = true, null)
    //
    //    // mapred-site.xml
    //    names = conf.getStringList("hadoop.v1.mapred-site.names").asScala.toList
    //    values = conf.getStringList("hadoop.v1.mapred-site.values").asScala.toList
    //    confString = configTemplate(names, values)
    //    scala.xml.XML.save(configPath + "mapred-site.xml", confString, "UTF-8", logOutput = true, null)
  }

}
