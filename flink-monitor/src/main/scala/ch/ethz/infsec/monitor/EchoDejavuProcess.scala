package ch.ethz.infsec
package monitor

import ch.ethz.infsec.trace.{KeyedDejavuPrinter, KeyedMonpolyPrinter, Record}

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

  override def readResults(buffer: mutable.Buffer[String]): Unit = {
    val line = reader.readLine()
    if (line != null)
      buffer += line
  }

  override def drainResults(buffer: mutable.Buffer[String]): Unit = readResults(buffer)

  override def readSnapshot(): Array[Byte] = Array.emptyByteArray
  override def readSnapshots(): Iterable[(Int, Array[Byte])] = ListSet.empty

}

//object EchoProcess{
//  def apply(cmd:Seq[String]):ExternalProcess[MonitorRequest,String] = new EchoProcess(cmd).asInstanceOf[ExternalProcess[MonitorRequest,String]]
//}

class EchoDejavuProcessFactory(cmd: Seq[String]) extends ExternalProcessFactory[(Int, Record), MonitorRequest, String, String] {
  override protected def createPre[MonpolyRequest >: MonitorRequest](): Processor[(Int, Record), MonitorRequest] = new KeyedDejavuPrinter[Int]
  override protected def createProc[MonpolyRequest >: MonitorRequest](): ExternalProcess[MonitorRequest, String] = new EchoDejavuProcess(cmd)
  override protected def createPost(): Processor[String, String] = StatelessProcessor.identity[String]
}
object EchoDejavuProcessFactory{
  def apply(cmd: Seq[String]): EchoDejavuProcessFactory = new EchoDejavuProcessFactory(cmd)
}
