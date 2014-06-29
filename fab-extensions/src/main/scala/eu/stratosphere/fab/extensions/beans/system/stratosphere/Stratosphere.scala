package eu.stratosphere.fab.extensions.beans.system.stratosphere

import java.io.File

import com.samskivert.mustache.Mustache
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.config.SystemConfig

class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends ExperimentRunner("Stratosphere", lifespan, dependencies, mc) {

  override def setUp(): Unit = {
    logger.info(s"Starting '$toString'")
  }

  override def tearDown(): Unit = {
    logger.info(s"Tearing down '$toString'")
  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")
  }

  override def run(job: String, input: List[File], output: File) = {
    logger.info("Running Stratosphere job")
  }

  override def configuration() = SystemConfig(config, Nil)
}
