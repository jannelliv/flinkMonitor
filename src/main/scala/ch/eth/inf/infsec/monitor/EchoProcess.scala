package ch.eth.inf.infsec.monitor

import scala.collection.mutable

class EchoProcess(val command: Seq[String]) extends AbstractExternalProcess[String, String] {
  override def open(): Unit = open(command)

  override def open(initialState: Array[Byte]): Unit = open()

  override def writeRequest(in: String): Unit = {
    writer.write(in)
    writer.flush()
  }

  override def initSnapshot(): Unit = ()

  override def readResults(buffer: mutable.Buffer[String]): Unit = {
    val line = reader.readLine()
    if (line != null)
      buffer += line
  }

  override def drainResults(buffer: mutable.Buffer[String]): Unit = readResults(buffer)

  override def readSnapshot(): Array[Byte] = Array.emptyByteArray
}
