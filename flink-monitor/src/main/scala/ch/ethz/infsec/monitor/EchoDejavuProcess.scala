package ch.ethz.infsec
package monitor

import ch.ethz.infsec.trace.{KeyedDejavuPrinter, Record}

import scala.collection.immutable.ListSet
import scala.collection.mutable

class EchoDejavuProcess(override val command: Seq[String]) extends DejavuProcess(command) {
  override def open(): Unit = open(command)

  override def open(initialState: Array[Byte]): Unit = open()
  override def open(initialStates: Iterable[(Int, Array[Byte])]): Unit = open()

  override def writeRequest[SubRequest >: DejavuRequest](request: SubRequest): Unit = {
    val r = request.asInstanceOf[DejavuRequest]
    writer.write(r.in)
    writer.flush()
  }

  override def initSnapshot(): Unit = ()
  override def initSnapshot(slicer: String): Unit = ()

  override def readResults(buffer: mutable.Buffer[MonitorResponse]): Unit = {
    val line = reader.readLine()
    if (line != null)
      buffer += VerdictItem(line)
  }

  override def drainResults(buffer: mutable.Buffer[MonitorResponse]): Unit = readResults(buffer)

  override def readSnapshot(): Array[Byte] = Array.emptyByteArray
  override def readSnapshots(): Iterable[(Int, Array[Byte])] = ListSet.empty

}

//object EchoProcess{
//  def apply(cmd:Seq[String]):ExternalProcess[MonitorRequest,String] = new EchoProcess(cmd).asInstanceOf[ExternalProcess[MonitorRequest,String]]
//}

class EchoDejavuProcessFactory(cmd: Seq[String]) extends MonitorFactory {
  override def createPre[T,MonpolyRequest >: MonitorRequest](): Processor[Either[(Int, Record),T], Either[MonitorRequest,T]] = new KeyedDejavuPrinter[Int,T]
  override def createProc[MonpolyRequest >: MonitorRequest](): ExternalProcess[MonitorRequest, MonitorResponse] = new EchoDejavuProcess(cmd)
  override def createPost(): Processor[MonitorResponse, MonitorResponse] = StatelessProcessor.identity[MonitorResponse]
}
object EchoDejavuProcessFactory{
  def apply(cmd: Seq[String]): EchoDejavuProcessFactory = new EchoDejavuProcessFactory(cmd)
}
