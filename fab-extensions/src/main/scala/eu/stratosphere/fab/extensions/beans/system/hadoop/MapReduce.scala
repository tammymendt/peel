package eu.stratosphere.fab.extensions.beans.system.hadoop

import java.io.File

import com.github.mustachejava.MustacheFactory
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}

class MapReduce(lifespan: Lifespan, dependencies: Set[System] = Set(), mf: MustacheFactory) extends ExperimentRunner("MapReduce", lifespan, dependencies, mf) {

//  val home: String = config.getString("paths.hadoop.v1.home")

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'...")
//    Shell.execute(home + "bin/start-mapred.sh", true)
  }

  override def tearDown(): Unit = {
    logger.info(s"Tearing down system '$toString'...")
//    Shell.execute(home + "bin/stop-mapred.sh", true)
  }

  override def update(): Unit = {
    logger.info(s"Updating system '$toString'...")
  }

  override def run(job: String, input: List[File], output: File) = {
//    logger.info("Running Hadoop Job %s %s %s".format(job, input.mkString(" "), output))
//    Shell.execute(home + "bin/hadoop jar %s %s %s".format(job, input.mkString(" "), output), true)
  }
}
