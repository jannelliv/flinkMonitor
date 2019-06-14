package ch.ethz.infsec
package monitor

import scala.collection.mutable
import ch.ethz.infsec.trace.{KeyedDejavuPrinter, DejavuVerdictFilter, Record}


class DejavuProcess(val command: Seq[String]) extends AbstractExternalProcess[DejavuRequest, String] {

  override def open(): Unit = {
    open(command)
    println("Opened DejaVu")
  }

  override def open(initialState: Array[Byte]): Unit = throw new UnsupportedOperationException

  override def open(initialState: Iterable[(Int, Array[Byte])]): Unit = throw new UnsupportedOperationException

  override def writeRequest[SubRequest >: DejavuRequest](req: SubRequest): Unit = {
    val r = req.asInstanceOf[DejavuRequest]
    r match {
      case DejavuCommandItem(cmd) => ()
      case DejavuEventItem(ev) => {
        writer.write(ev)
        writer.flush()
      }
    }
  }

  override def initSnapshot(): Unit = throw new UnsupportedOperationException

  override def initSnapshot(slicer: String): Unit = throw new UnsupportedOperationException

  override def readResults(buffer: mutable.Buffer[String]): Unit = {
    val line = reader.readLine()
    buffer += line
  }

  override def drainResults(buffer: mutable.Buffer[String]): Unit = {
    readResultsUntil(buffer, s => false)
  }


  private def readResultsUntil(buffer:mutable.Buffer[String], until: String => Boolean):Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line == null || until(line)) {
        more = false
      } else {
        buffer += line
      }
    } while (more)
  }


  override def readSnapshot(): Array[Byte] =  throw new UnsupportedOperationException

  override def readSnapshots(): Iterable[(Int, Array[Byte])] =  throw new UnsupportedOperationException

  override val SYNC_BARRIER_IN: DejavuRequest = DejavuEventItem(DejavuProcess.SYNC)
  override val SYNC_BARRIER_OUT: String => Boolean = _ == DejavuProcess.SYNC_OUT
}

object DejavuProcess {
  val VIOLATION_PREFIX = "**** Property violated on event number "
  val GET_INDEX_PREFIX = "**** Time point "
  val SYNC = "SYNC!"
  val SYNC_OUT = "**** " + SYNC
}


class DejavuProcessFactory(cmd: Seq[String]) extends ExternalProcessFactory[(Int, Record), MonitorRequest, String, String] {
  override protected def createPre[DejavuRequest >: MonitorRequest](): Processor[(Int, Record), MonitorRequest] = new KeyedDejavuPrinter[Int]
  override protected def createProc[DejavuRequest >: MonitorRequest](): ExternalProcess[MonitorRequest, String] = new DejavuProcess(cmd)
  override protected def createPost(): Processor[String, String] = new DejavuVerdictFilter()
}

object DejavuProcessFactory {
  def apply(cmd: Seq[String]): DejavuProcessFactory = new DejavuProcessFactory(cmd)
}