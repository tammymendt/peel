package eu.stratosphere.fab.core.config

import com.typesafe.config.Config
import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer


object namevalue {

  case class Pair(name: String, value: Any) {}

  class Context(val c: Config, val prefix: String) {

    val properties = {

      val builder = ListBuffer[Pair]()

      def collect(c: Config): Unit = {
        for (e <- c.entrySet().asScala) e.getValue match {
          case c: Config => collect(c)
          case _ => builder += Pair(e.getKey.stripPrefix(s"$prefix."), c.getString(e.getKey))
        }
      }

      collect(c.withOnlyPath(prefix))

      builder.toList.asJava
    }
  }

}
