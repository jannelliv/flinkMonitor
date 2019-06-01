package ch.ethz.infsec.monitor
import scala.collection.mutable

abstract class DirectProcess[IN,OUT](val command: Seq[String]) extends AbstractExternalProcess [IN,OUT]{
  override def open(): Unit = {
    open(command)
    println("Opened " + command.mkString)
  }

  override def open(initialState: Array[Byte]): Unit = throw new UnsupportedOperationException

  override def open(initialState: Iterable[(Int, Array[Byte])]): Unit = throw new UnsupportedOperationException

  override def writeRequest[UIN >: IN](in: UIN): Unit = {
    writer.write(in.toString)
    writer.flush()
  }

  override def initSnapshot(): Unit = throw new UnsupportedOperationException

  override def initSnapshot(slicer: String): Unit = throw new UnsupportedOperationException

  def parseLine(line: String): OUT

  def isEndMarker(t: OUT):Boolean

  private def readResultsUntil(buffer:mutable.Buffer[OUT], until: String => Boolean):Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line == null || until(line)) {
        more = false
      } else {
        buffer += parseLine(line)
      }
    } while (more)
  }

  override def readResults(buffer: mutable.Buffer[OUT]): Unit = {
    readResultsUntil(buffer,x=>isEndMarker(parseLine(x)))
  }

  override def drainResults(buffer: mutable.Buffer[OUT]): Unit = {
    readResultsUntil(buffer,t => false)
  }

  override def readSnapshot(): Array[Byte] = throw new UnsupportedOperationException

  override def readSnapshots(): Iterable[(Int, Array[Byte])] = throw new UnsupportedOperationException
}

abstract class DirectProcessFactory[IN,OUT] extends DirectExternalProcessFactory[IN,OUT]{

}