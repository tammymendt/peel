package eu.stratosphere.fab.extensions.beans.system.stratosphere

import java.io.File

import com.samskivert.mustache.Mustache
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
<<<<<<< HEAD
import java.io.{FileWriter, File}
import eu.stratosphere.fab.core.beans.Shell
import com.typesafe.config.Config
import scala.collection.JavaConverters._
=======
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.config.SystemConfig
>>>>>>> 7fdfbf59d999eb285a1db89062a7d90708f498b8

class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends ExperimentRunner("Stratosphere", lifespan, dependencies, mc) {

<<<<<<< HEAD
  val home: String = config.getString("paths.stratosphere.v5.home")

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
    val src: String = config.getString("paths.stratosphere.v5.source")
    val target: String = config.getString("paths.stratosphere.v5.target")
    val user: String = config.getString("hdfs.v1.user.name")
    val group: String = config.getString("hdfs.v1.user.group")
    if (new File(home).exists) Shell.rmDir(home)
    Shell.untar(src, target)
    Shell.execute(("chown -R %s:%s " + home).format(user, group) , true)
    configure(config)
    Shell.execute(home + "bin/start-local.sh", true)
  }

  def run(job: String, input: List[File], output: File) = {
    logger.info("Running Stratosphere Job...")
    Shell.execute(home + "bin/stratosphere run %s %s %s".format(job, input.mkString(" "), output), true)
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
    Shell.rmDir(home)
=======
  override def setUp(): Unit = {
    logger.info(s"Starting '$toString'")
  }

  override def tearDown(): Unit = {
    logger.info(s"Tearing down '$toString'")
  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")
>>>>>>> 7fdfbf59d999eb285a1db89062a7d90708f498b8
  }

  override def run(job: String, input: List[File], output: File) = {
    logger.info("Running Stratosphere job")
  }

<<<<<<< HEAD
  def configure(conf: Config) = {
    val fw = new FileWriter(home + "conf/stratosphere-conf.yaml", true)
    val names = conf.getStringList("stratosphere.v5.stratosphere-conf.names").asScala.toList
    val values = conf.getStringList("stratosphere.v5.stratosphere-conf.values").asScala.toList
    if(names.length != values.length)
      throw new RuntimeException("Name and Value Lists must have same number of elements!")

    try {
      (names, values).zipped.map{(n, v) => fw.write(n + ": " + v)}
    }
    finally fw.close()
  }

  override def toString = "Stratosphere v0.5"
=======
  override def configuration() = SystemConfig(config, Nil)
>>>>>>> 7fdfbf59d999eb285a1db89062a7d90708f498b8
}
