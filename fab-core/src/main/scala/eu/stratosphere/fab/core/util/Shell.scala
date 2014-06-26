package eu.stratosphere.fab.core.util

import org.slf4j.LoggerFactory

import scala.sys.process.{Process, ProcessLogger}

object Shell {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def execute(str: String, logOutput: Boolean = false): (String, String, Int) = {
    logger.debug("Executing: " + str)

    val out = new StringBuilder
    val err = new StringBuilder

    // Use ProcessLogger to catch the results of stdout and strerr
    // Use bash to enable the use of bash features (e.g. wildcards)
    val exitcode = Process("/bin/bash", Seq("-c", str)) ! ProcessLogger(
      (s) => out.append(s + "\n"),
      (s) => err.append(s + "\n"))

    if (logger.isDebugEnabled) {
      if (!out.toString().trim.isEmpty)
        logger.debug(" - result stdout: " + out)
      if (!err.toString().trim.isEmpty)
        logger.debug(" - result strerr: " + err)
    }

    (out.toString(), err.toString(), exitcode)
  }

  def rmDir(path: String) = {
    execute(s"rm -r $path")
  }

  def untar(src: String, dst: String) = {
    execute(s"tar -xzf $src -C $dst")
  }
}
