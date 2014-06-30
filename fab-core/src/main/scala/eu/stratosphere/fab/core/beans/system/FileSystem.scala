package eu.stratosphere.fab.core.beans.system

import java.io.File
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

trait FileSystem {

  def setInput(from: File): File

  def getOutput(from: File, to: File)
}
