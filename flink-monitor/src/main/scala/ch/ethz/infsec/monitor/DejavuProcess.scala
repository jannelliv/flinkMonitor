package ch.ethz.infsec
package monitor

import ch.ethz.infsec.trace.{DejavuVerdictFilter, KeyedDejavuPrinter, LiftProcessor, Record}

import scala.collection.mutable


class DejavuProcess(val command: Seq[String]) extends AbstractExternalProcess[DejavuRequest, MonitorResponse] {

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
        if (ev==DejavuProcess.SYNC)
          writer.flush()
      }
    }
  }

  override def initSnapshot(): Unit = throw new UnsupportedOperationException

  override def initSnapshot(slicer: String): Unit = throw new UnsupportedOperationException

  override def readResults(buffer: mutable.Buffer[MonitorResponse]): Unit = {
    val line = reader.readLine()
    buffer += VerdictItem(line)
  }

  override def drainResults(buffer: mutable.Buffer[MonitorResponse]): Unit = {
    readResultsUntil(buffer, s => false)
  }


  private def readResultsUntil(buffer:mutable.Buffer[MonitorResponse], until: String => Boolean):Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line == null || until(line)) {
        more = false
      } else {
        buffer += VerdictItem(line)      }
    } while (more)
  }


  override def readSnapshot(): Array[Byte] =  throw new UnsupportedOperationException

  override def readSnapshots(): Iterable[(Int, Array[Byte])] =  throw new UnsupportedOperationException

  override val SYNC_BARRIER_IN: DejavuRequest = DejavuEventItem(DejavuProcess.SYNC)
  override val SYNC_BARRIER_OUT: MonitorResponse => Boolean = _.in == DejavuProcess.SYNC_OUT
}

object DejavuProcess {
  val VIOLATION_PREFIX = "**** Property violated on event number "
  val GET_INDEX_PREFIX = "**** Time point "
  val SYNC = "SYNC!\n"
  val SYNC_OUT = "**** SYNC!"
}


class DejavuProcessFactory(cmd: Seq[String]) extends MonitorFactory {
  override def createPre[T,DejavuRequest >: MonitorRequest](): Processor[Either[(Int, Record),T], Either[MonitorRequest,T]] = new KeyedDejavuPrinter[Int,T]
  override def createProc[DejavuRequest >: MonitorRequest](): ExternalProcess[MonitorRequest, MonitorResponse] = new DejavuProcess(cmd)
  override def createPost(): Processor[MonitorResponse, MonitorResponse] = new LiftProcessor(new DejavuVerdictFilter())
}

object DejavuProcessFactory {
  def apply(cmd: Seq[String]): DejavuProcessFactory = new DejavuProcessFactory(cmd)
}
