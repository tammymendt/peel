package eu.stratosphere.fab.core.beans.system

case object Lifespan extends Enumeration {
  type Lifespan = Value
  final val SUITE, EXPERIMENT = Value
}
