package eu.stratosphere.peel.core.beans.experiment

import java.lang.{System => Sys}
import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.system.System
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.Node
import eu.stratosphere.peel.core.util.shell
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

abstract class Experiment[+R <: System](val command: String,
                                        val runner: R,
                                        val runs: Int,
                                        val inputs: Set[DataSet],
                                        val outputs: Set[ExperimentOutput],
                                        val name: String,
                                        var config: Config) extends Node with Configurable {

  /**
   * Experiment run factory method.
   *
   * @param id The `id` for the constructed experiment run
   * @param force Force execution of this run
   * @return An run for this experiment identified by the given `id`
   */
  def run(id: Int, force: Boolean): Experiment.Run[R]

  /**
   * Alias of name.
   *
   * @return
   */
  override def toString: String = name
}

object Experiment {

  trait Run[+R <: System] {

    final val logger = LoggerFactory.getLogger(this.getClass)

    val id: Int
    val exp: Experiment[R]
    val force: Boolean

    val home = f"${exp.config.getString("app.path.results")}/${exp.config.getString("app.suite.name")}/${exp.name}.run$id%02d"
    val name = f"${exp.name}.run$id%02d"

    // ensure experiment folder structure in the constructor
    {
      ensureFolderIsWritable(Paths.get(s"$home"))
      ensureFolderIsWritable(Paths.get(s"$home/logs"))
    }

    def isSuccessful: Boolean

    def execute(): Unit

    protected final def ensureFolderIsWritable(folder: Path) = {
      if (Files.exists(folder)) {
        if (!(Files.isDirectory(folder) && Files.isWritable(folder))) throw new RuntimeException(s"Experiment home '$home' is not a writable directory")
      } else {
        Files.createDirectories(folder)
      }
    }
  }

  trait RunState {
    val name: String
    var runExitCode: Option[Int]
    var runTime: Long
  }

  /**
   * A private inner class encapsulating the logic of single run.
   */
  trait SingleJobRun[+R <: System, RS <: RunState] extends Run[R] {

    var state = loadState()

    var logFileCounts: Map[String, Long] = null

    override def execute() = {
      if (!force && isSuccessful) {
        logger.info("Skipping successfully finished experiment run %s".format(name))
      } else {
        logger.info("Running experiment %s".format(name))
        logger.info("Experiment data will be written to %s".format(home))
        logger.info("Experiment command is %s".format(command))

        try {

          beforeRun()

          try {
            Await.ready(future(runJob()), exp.config.getLong("experiment.timeout") seconds)
          } catch {
            case e: TimeoutException =>
              logger.warn(s"Experiment run did not finish within the given time limit of ${exp.config.getLong("experiment.timeout")} seconds")
              cancelJob()
            case e: InterruptedException =>
              logger.warn(s"Experiment run was interrupted")
              cancelJob()
            case e: Throwable =>
              logger.warn(s"Experiment run threw an unexpected exception: ${e.getMessage}")
              cancelJob()
          }

          afterRun()

          if (isSuccessful)
            logger.info(s"Experiment run finished in ${state.runTime} milliseconds")
          else
            logger.warn(s"Experiment run did not finish successfully")
        } catch {
          case e: Exception => logger.error("Exception in experiment run %s: %s".format(name, e.getMessage))
        } finally {
          writeState()
        }
      }
    }

    protected def beforeRun() = {
      // collect runner log files and their current line counts
      val logFiles = for (pattern <- logFilePatterns; f <- (shell !! s"ls $pattern").split(Sys.lineSeparator).map(_.trim)) yield f
      logFileCounts = Map((for (f <- logFiles) yield f -> (shell !! s"wc -l $f | cut -d' ' -f1").trim.toLong): _*)
    }

    protected def afterRun(): Unit = {
      // copy logs
      shell ! s"rm -Rf $home/logs/*"
      for ((file, count) <- logFileCounts) shell ! s"tail -n +${count + 1} $file > $home/logs/${Paths.get(file).getFileName}"
    }

    protected def command = exp.resolve(exp.command)

    protected def logFilePatterns: List[String]

    protected def loadState(): RS

    protected def writeState(): Unit

    protected def runJob(): Unit

    protected def cancelJob(): Unit
  }

  def time[T](block: => T): (T, Long) = {
    val t0 = Sys.currentTimeMillis
    val result = block
    val t1 = Sys.currentTimeMillis
    (result, t1 - t0)
  }
}
