package eu.stratosphere.fab.core.beans.system

import java.io.File

import com.github.mustachejava.MustacheFactory
import eu.stratosphere.fab.core.beans.system.Lifespan._

abstract class ExperimentRunner(name: String, lifespan: Lifespan, dependencies: Set[System], mf: MustacheFactory) extends System("stratosphere", lifespan, dependencies, mf) {
  def run(job: String, input: List[File], output: File)
}
