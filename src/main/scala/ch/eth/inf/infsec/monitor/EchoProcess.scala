package ch.eth.inf.infsec.monitor

import scala.collection.immutable.ListSet
import scala.collection.mutable

class EchoProcess(val command: Seq[String]) extends AbstractExternalProcess[MonpolyRequest, MonpolyRequest] {
  override def open(): Unit = open(command)

  override def open(initialState: Array[Byte]): Unit = open()
  override def open(initialStates: Iterable[(Int, Array[Byte])]): Unit = open()

  private val isCommandQueue = new mutable.Queue[Boolean]()

  override def writeRequest(request: MonpolyRequest): Unit = {
    writer.write(request.in)
    request match {
      case CommandItem(_) =>     isCommandQueue.enqueue(true)
      case _ => isCommandQueue.enqueue(false)
    }

    writer.flush()
  }

  override def initSnapshot(): Unit = ()
  override def initSnapshot(slicer: String): Unit = ()

  override def readResults(buffer: mutable.Buffer[MonpolyRequest]): Unit = {
    val line = reader.readLine()
    if (line != null) {
      if(isCommandQueue.dequeue())
        buffer += CommandItem(line)
      else
        buffer += EventItem(line)
    }
  }

  override def drainResults(buffer: mutable.Buffer[MonpolyRequest]): Unit = readResults(buffer)

  override def readSnapshot(): Array[Byte] = Array.emptyByteArray
  override def readSnapshots(): Iterable[(Int, Array[Byte])] = ListSet.empty
}
