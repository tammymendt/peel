package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.System
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set()) extends System(lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

}