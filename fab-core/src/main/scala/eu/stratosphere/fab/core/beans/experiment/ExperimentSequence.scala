package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.context.ExecutionContext

import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

class ExperimentSequence(val e: Experiment) {
  final val logger = LoggerFactory.getLogger(this.getClass)

  def run(ctx: ExecutionContext) = {

  }

}
