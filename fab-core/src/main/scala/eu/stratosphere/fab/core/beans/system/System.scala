package eu.stratosphere.fab.core.beans.system

import com.github.mustachejava.MustacheFactory
import com.typesafe.config.Config
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.graph.Node
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

abstract class System(val defaultName: String, val lifespan: Lifespan, val dependencies: Set[System], val mf: MustacheFactory) extends Node with BeanNameAware {

  import scala.language.implicitConversions

  final val logger = LoggerFactory.getLogger(this.getClass)

  implicit var config: Option[Config] = None

  var name = defaultName

  def setUp(): Unit

  def tearDown(): Unit

  def update(): Unit

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  val configTemplate = { (names: List[String], values: List[String]) =>
    if (names.length != values.length)
      throw new RuntimeException("Name and Value Lists must have same number of elements!")

    <configuration>
      {(names, values).zipped.map { (n, v) =>
      <property>
        <name>
          {n}
        </name>
        <value>
          {v}
        </value>
      </property>
    }}
    </configuration>
  }

  val envTemplate = { (names: List[String], values: List[String]) =>
    if (names.length != values.length)
      throw new RuntimeException("Name and Value Lists must have same number of elements!")
    (names, values).zipped.map { (n, v) => "export %s=\"%s\" \n".format(n, v)}
  }

  override def setBeanName(n: String) = name = n

  override def toString: String = name
}
