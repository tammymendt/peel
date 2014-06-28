package eu.stratosphere.fab.extensions.beans.system.hadoop

import java.io.File

import com.samskivert.mustache.Mustache
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.config.SystemConfig

class MapReduce(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends ExperimentRunner("MapReduce", lifespan, dependencies, mc) {

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'...")
  }

  override def tearDown(): Unit = {
    logger.info(s"Tearing down system '$toString'...")
  }

  override def update(): Unit = {
    logger.info(s"Updating system '$toString'...")
  }

  override def run(job: String, input: List[File], output: File) = {
    logger.info("Running MapReduce job")
//    Shell.execute(home + "bin/hadoop jar %s %s %s".format(job, input.mkString(" "), output), true)
  }

  override def configuration() = SystemConfig(config.get, Nil)
}
