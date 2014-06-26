package eu.stratosphere.fab.extensions.beans.system.stratosphere

import java.io.File

import com.github.mustachejava.MustacheFactory
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set(), mf: MustacheFactory) extends ExperimentRunner("Stratosphere", lifespan, dependencies, mf) {

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
    logger.info("Running Stratosphere Job...")
  }
}
